// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/unsafe2"
)

type Proxy struct {
	mu sync.Mutex //互斥锁

	xauth string        //账号授权信息
	model *models.Proxy //proxy的基本设置

	exit struct { //用于关闭
		C chan struct{}
	}
	online bool //是否在线
	closed bool //是否关闭

	config *Config //配置参数
	router *Router //Router中比较重要的是连接池和slots
	ignore []byte  //设置堆占位符以减少GC频率。

	lproxy net.Listener //proxy的监听处理逻辑，默认端口19000
	ladmin net.Listener //admin的监听处理逻辑，默认端口11080

	ha struct { //高可用相关、哨兵监听
		monitor *redis.Sentinel
		masters map[int]string
		servers []string
	}
	jodis *Jodis //jodis连接
}

var ErrClosedProxy = errors.New("use of closed proxy")

func New(config *Config) (*Proxy, error) {
	// 检查配置属性是否合法，包括空检查和值范围检查
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	// productName 命名规范，只能是小写字母和-的组合
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}

	s := &Proxy{}
	s.config = config
	s.exit.C = make(chan struct{})
	// 创建路由连接池子，初始化 1024 个 slots
	s.router = NewRouter(config)
	s.ignore = make([]byte, config.ProxyHeapPlaceholder.Int64())

	s.model = &models.Proxy{
		StartTime: time.Now().String(),
	}
	s.model.ProductName = config.ProductName
	s.model.DataCenter = config.ProxyDataCenter
	s.model.Pid = os.Getpid()
	s.model.Pwd, _ = os.Getwd()
	// 获取系统信息
	// Darwin sanyuedeMacBook-Pro.local 22.3.0 Darwin Kernel Version 22.3.0: Mon Jan 30 20:42:11 PST 2023;
	// root:xnu-8792.81.3~2/RELEASE_X86_64 x86_64
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}
	s.model.Hostname = utils.Hostname

	if err := s.setup(config); err != nil { // 1、启动服务监听 admin(:11080) 和 proxy(:19000) 的请求；2、初始化auth鉴权
		s.Close()
		return nil, err
	}

	log.Warnf("[%p] create new proxy:\n%s", s, s.model.Encode())

	unsafe2.SetMaxOffheapBytes(config.ProxyMaxOffheapBytes.Int64())

	go s.serveAdmin() // 创建后台接口服务，提供给 dashboard使用，监听端口 11080
	go s.serveProxy() // 创建proxy服务，监听端口 19000

	// 定时上报 metric 信息给指端的 server 端服务器。
	// 【拓展】后面可以基于此做监控
	s.startMetricsJson()
	s.startMetricsInfluxdb()
	s.startMetricsStatsd()

	return s, nil
}

// 绑定 admin 和 proxy 的监听端口，但是没有加处理逻辑
func (s *Proxy) setup(config *Config) error {
	proto := config.ProtoType                                      // tcp4
	if l, err := net.Listen(proto, config.ProxyAddr); err != nil { // ProxyAddr=0.0.0.0:19000，19000 也是 server 服务的监听端口，Set bind address for proxy
		return errors.Trace(err)
	} else {
		s.lproxy = l

		x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostProxy) // l.Addr().String()=0.0.0.0:19000 config.HostProxy=tcp4
		if err != nil {
			return err
		}
		s.model.ProtoType = proto // tcp4
		s.model.ProxyAddr = x     // sanyuedeMacBook-Pro.local:19000
	}

	proto = "tcp"
	if l, err := net.Listen(proto, config.AdminAddr); err != nil { // proto=tcp, config.AdminAddr=0.0.0.0:11080，Set bind address for admin(rpc), tcp only.
		return errors.Trace(err)
	} else {
		s.ladmin = l

		x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostAdmin) // x=sanyuedeMacBook-Pro.local:11080
		if err != nil {
			return err
		}
		s.model.AdminAddr = x
	}

	s.model.Token = rpc.NewToken(
		config.ProductName,
		s.lproxy.Addr().String(),
		s.ladmin.Addr().String(),
	)
	s.xauth = rpc.NewXAuth( // 初始化auth鉴权
		config.ProductName,
		config.ProductAuth,
		s.model.Token,
	)

	if config.JodisAddr != "" {
		c, err := models.NewClient(config.JodisName, config.JodisAddr, config.JodisAuth, config.JodisTimeout.Duration())
		if err != nil {
			return err
		}
		if config.JodisCompatible {
			s.model.JodisPath = filepath.Join("/zk/codis", fmt.Sprintf("db_%s", config.ProductName), "proxy", s.model.Token)
		} else {
			s.model.JodisPath = models.JodisPath(config.ProductName, s.model.Token)
		}
		s.jodis = NewJodis(c, s.model)
	}

	return nil
}

func (s *Proxy) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	if s.online {
		return nil
	}
	s.online = true
	s.router.Start()
	if s.jodis != nil {
		s.jodis.Start()
	}
	return nil
}

func (s *Proxy) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.jodis != nil {
		s.jodis.Close()
	}
	if s.ladmin != nil {
		s.ladmin.Close()
	}
	if s.lproxy != nil {
		s.lproxy.Close()
	}
	if s.router != nil {
		s.router.Close()
	}
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
	}
	return nil
}

func (s *Proxy) XAuth() string {
	return s.xauth
}

func (s *Proxy) Model() *models.Proxy {
	return s.model
}

func (s *Proxy) Config() *Config {
	return s.config
}

func (s *Proxy) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *Proxy) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Proxy) HasSwitched() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.router.HasSwitched()
}

func (s *Proxy) Slots() []*models.Slot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.router.GetSlots()
}

func (s *Proxy) FillSlot(m *models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	return s.router.FillSlot(m)
}

func (s *Proxy) FillSlots(slots []*models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	for _, m := range slots {
		if err := s.router.FillSlot(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Proxy) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	s.ha.masters = masters

	if len(masters) != 0 {
		s.router.SwitchMasters(masters)
	}
	return nil
}

func (s *Proxy) GetSentinels() ([]string, map[int]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil
	}
	return s.ha.servers, s.ha.masters
}

func (s *Proxy) SetSentinels(servers []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	s.ha.servers = servers
	log.Warnf("[%p] set sentinels = %v", s, s.ha.servers)

	s.rewatchSentinels(s.ha.servers)
	return nil
}

func (s *Proxy) RewatchSentinels() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	log.Warnf("[%p] rewatch sentinels = %v", s, s.ha.servers)

	s.rewatchSentinels(s.ha.servers)
	return nil
}

func (s *Proxy) rewatchSentinels(servers []string) {
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
		s.ha.monitor = nil
		s.ha.masters = nil
	}
	if len(servers) != 0 {
		s.ha.monitor = redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
		s.ha.monitor.LogFunc = log.Warnf
		s.ha.monitor.ErrFunc = log.WarnErrorf
		go func(p *redis.Sentinel) {
			var trigger = make(chan struct{}, 1)
			delayUntil := func(deadline time.Time) {
				for !p.IsCanceled() {
					var d = deadline.Sub(time.Now())
					if d <= 0 {
						return
					}
					time.Sleep(math2.MinDuration(d, time.Second))
				}
			}
			go func() {
				defer close(trigger)
				callback := func() {
					select {
					case trigger <- struct{}{}:
					default:
					}
				}
				for !p.IsCanceled() {
					timeout := time.Minute * 15
					retryAt := time.Now().Add(time.Second * 10)
					if !p.Subscribe(servers, timeout, callback) {
						delayUntil(retryAt)
					} else {
						callback()
					}
				}
			}()
			go func() {
				for range trigger {
					var success int
					for i := 0; i != 10 && !p.IsCanceled() && success != 2; i++ {
						timeout := time.Second * 5
						masters, err := p.Masters(servers, timeout)
						if err != nil {
							log.WarnErrorf(err, "[%p] fetch group masters failed", s)
						} else {
							if !p.IsCanceled() {
								s.SwitchMasters(masters)
							}
							success += 1
						}
						delayUntil(time.Now().Add(time.Second * 5))
					}
				}
			}()
		}(s.ha.monitor)
	}
}

// 创建后台接口服务，提供给 dashboard使用，监听端口 11080
func (s *Proxy) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("[%p] admin start service on %s", s, s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("[%p] admin shutdown", s)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] admin exit on error", s)
	}
}

func (s *Proxy) serveProxy() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("[%p] proxy start service on %s", s, s.lproxy.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) (err error) {
		defer func() {
			eh <- err
		}()
		for {
			// 接收accept请求。对于client的每个连接，只会打开一个Session
			c, err := s.acceptConn(l)
			if err != nil {
				return err
			}
			//创建并启动session处理
			NewSession(c, s.config).Start(s.router)
		}
	}(s.lproxy)

	if d := s.config.BackendPingPeriod.Duration(); d != 0 {
		// Router可用检测
		go s.keepAlive(d)
	}

	// 夯住协程，直到程序退出，或是发生错误
	select {
	case <-s.exit.C:
		log.Warnf("[%p] proxy shutdown", s)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] proxy exit on error", s)
	}
}

func (s *Proxy) keepAlive(d time.Duration) {
	var ticker = time.NewTicker(math2.MaxDuration(d, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-s.exit.C:
			return
		case <-ticker.C:
			s.router.KeepAlive()
		}
	}
}

func (s *Proxy) acceptConn(l net.Listener) (net.Conn, error) {
	var delay = &DelayExp2{
		Min: 10, Max: 500,
		Unit: time.Millisecond,
	}
	for {
		c, err := l.Accept()
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed", s)
				delay.Sleep()
				continue
			}
		}
		return c, err
	}
}

type Overview struct {
	Version string         `json:"version"`
	Compile string         `json:"compile"`
	Config  *Config        `json:"config,omitempty"`
	Model   *models.Proxy  `json:"model,omitempty"`
	Stats   *Stats         `json:"stats,omitempty"`
	Slots   []*models.Slot `json:"slots,omitempty"`
}

type Stats struct {
	Online bool `json:"online"`
	Closed bool `json:"closed"`

	Sentinels struct {
		Servers  []string          `json:"servers,omitempty"`
		Masters  map[string]string `json:"masters,omitempty"`
		Switched bool              `json:"switched,omitempty"`
	} `json:"sentinels"`

	Ops struct {
		Total int64 `json:"total"`
		Fails int64 `json:"fails"`
		Redis struct {
			Errors int64 `json:"errors"`
		} `json:"redis"`
		QPS int64      `json:"qps"`
		Cmd []*OpStats `json:"cmd,omitempty"`
	} `json:"ops"`

	Sessions struct {
		Total int64 `json:"total"`
		Alive int64 `json:"alive"`
	} `json:"sessions"`

	Rusage struct {
		Now string       `json:"now"`
		CPU float64      `json:"cpu"`
		Mem int64        `json:"mem"`
		Raw *utils.Usage `json:"raw,omitempty"`
	} `json:"rusage"`

	Backend struct {
		PrimaryOnly bool `json:"primary_only"`
	} `json:"backend"`

	Runtime *RuntimeStats `json:"runtime,omitempty"`
}

type RuntimeStats struct {
	General struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Lookups uint64 `json:"lookups"`
		Mallocs uint64 `json:"mallocs"`
		Frees   uint64 `json:"frees"`
	} `json:"general"`

	Heap struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Idle    uint64 `json:"idle"`
		Inuse   uint64 `json:"inuse"`
		Objects uint64 `json:"objects"`
	} `json:"heap"`

	GC struct {
		Num          uint32  `json:"num"`
		CPUFraction  float64 `json:"cpu_fraction"`
		TotalPauseMs uint64  `json:"total_pausems"`
	} `json:"gc"`

	NumProcs      int   `json:"num_procs"`
	NumGoroutines int   `json:"num_goroutines"`
	NumCgoCall    int64 `json:"num_cgo_call"`
	MemOffheap    int64 `json:"mem_offheap"`
}

type StatsFlags uint32

func (s StatsFlags) HasBit(m StatsFlags) bool {
	return (s & m) != 0
}

const (
	StatsCmds = StatsFlags(1 << iota)
	StatsSlots
	StatsRuntime

	StatsFull = StatsFlags(^uint32(0))
)

func (s *Proxy) Overview(flags StatsFlags) *Overview {
	o := &Overview{
		Version: utils.Version,
		Compile: utils.Compile,
		Config:  s.Config(),
		Model:   s.Model(),
		Stats:   s.Stats(flags),
	}
	if flags.HasBit(StatsSlots) {
		o.Slots = s.Slots()
	}
	return o
}

func (s *Proxy) Stats(flags StatsFlags) *Stats {
	stats := &Stats{}
	stats.Online = s.IsOnline()
	stats.Closed = s.IsClosed()

	servers, masters := s.GetSentinels()
	if servers != nil {
		stats.Sentinels.Servers = servers
	}
	if masters != nil {
		stats.Sentinels.Masters = make(map[string]string)
		for gid, addr := range masters {
			stats.Sentinels.Masters[strconv.Itoa(gid)] = addr
		}
	}
	stats.Sentinels.Switched = s.HasSwitched()

	stats.Ops.Total = OpTotal()
	stats.Ops.Fails = OpFails()
	stats.Ops.Redis.Errors = OpRedisErrors()
	stats.Ops.QPS = OpQPS()

	if flags.HasBit(StatsCmds) {
		stats.Ops.Cmd = GetOpStatsAll()
	}

	stats.Sessions.Total = SessionsTotal()
	stats.Sessions.Alive = SessionsAlive()

	if u := GetSysUsage(); u != nil {
		stats.Rusage.Now = u.Now.String()
		stats.Rusage.CPU = u.CPU
		stats.Rusage.Mem = u.MemTotal()
		stats.Rusage.Raw = u.Usage
	}

	stats.Backend.PrimaryOnly = s.Config().BackendPrimaryOnly

	if flags.HasBit(StatsRuntime) {
		var r runtime.MemStats
		runtime.ReadMemStats(&r)

		stats.Runtime = &RuntimeStats{}
		stats.Runtime.General.Alloc = r.Alloc
		stats.Runtime.General.Sys = r.Sys
		stats.Runtime.General.Lookups = r.Lookups
		stats.Runtime.General.Mallocs = r.Mallocs
		stats.Runtime.General.Frees = r.Frees
		stats.Runtime.Heap.Alloc = r.HeapAlloc
		stats.Runtime.Heap.Sys = r.HeapSys
		stats.Runtime.Heap.Idle = r.HeapIdle
		stats.Runtime.Heap.Inuse = r.HeapInuse
		stats.Runtime.Heap.Objects = r.HeapObjects
		stats.Runtime.GC.Num = r.NumGC
		stats.Runtime.GC.CPUFraction = r.GCCPUFraction
		stats.Runtime.GC.TotalPauseMs = r.PauseTotalNs / uint64(time.Millisecond)
		stats.Runtime.NumProcs = runtime.GOMAXPROCS(0)
		stats.Runtime.NumGoroutines = runtime.NumGoroutine()
		stats.Runtime.NumCgoCall = runtime.NumCgoCall()
		stats.Runtime.MemOffheap = unsafe2.OffheapBytes()
	}
	return stats
}
