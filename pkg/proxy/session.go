// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Session struct {
	Conn *redis.Conn

	Ops int64

	CreateUnix int64
	LastOpUnix int64

	database int32 // 使用的数据库。比如执行 SELECT db 语句，会直接把选中的db给赋值

	quit bool
	exit sync.Once

	stats struct {
		opmap map[string]*opStats
		total atomic2.Int64
		fails atomic2.Int64
		flush struct {
			n    uint
			nano int64
		}
	}
	start sync.Once

	broken atomic2.Bool
	config *Config

	authorized bool
}

func (s *Session) String() string {
	o := &struct {
		Ops        int64  `json:"ops"`
		CreateUnix int64  `json:"create"`
		LastOpUnix int64  `json:"lastop,omitempty"`
		RemoteAddr string `json:"remote"`
	}{
		s.Ops, s.CreateUnix, s.LastOpUnix,
		s.Conn.RemoteAddr(),
	}
	b, _ := json.Marshal(o)
	return string(b)
}

func NewSession(sock net.Conn, config *Config) *Session {
	c := redis.NewConn(sock,
		config.SessionRecvBufsize.AsInt(),
		config.SessionSendBufsize.AsInt(),
	)
	c.ReaderTimeout = config.SessionRecvTimeout.Duration()         //回复缓冲区超时时间。默认30m
	c.WriterTimeout = config.SessionSendTimeout.Duration()         //发送缓冲区超时时间，默认30s
	c.SetKeepAlivePeriod(config.SessionKeepAlivePeriod.Duration()) //连接超时，默认75s

	s := &Session{
		Conn: c, config: config,
		CreateUnix: time.Now().Unix(),
	}
	s.stats.opmap = make(map[string]*opStats, 16)
	log.Infof("session [%p] create: %s", s, s)
	return s
}

func (s *Session) CloseReaderWithError(err error) error {
	s.exit.Do(func() {
		if err != nil {
			log.Infof("session [%p] closed: %s, error: %s", s, s, err)
		} else {
			log.Infof("session [%p] closed: %s, quit", s, s)
		}
	})
	return s.Conn.CloseReader()
}

func (s *Session) CloseWithError(err error) error {
	s.exit.Do(func() {
		if err != nil {
			log.Infof("session [%p] closed: %s, error: %s", s, s, err)
		} else {
			log.Infof("session [%p] closed: %s, quit", s, s)
		}
	})
	s.broken.Set(true)
	return s.Conn.Close()
}

var (
	ErrRouterNotOnline          = errors.New("router is not online")
	ErrTooManySessions          = errors.New("too many sessions")
	ErrTooManyPipelinedRequests = errors.New("too many pipelined requests")
)

var RespOK = redis.NewString([]byte("OK"))

// 备注 router 只包含了 config 的信息
func (s *Session) Start(d *Router) {
	// start 说明一个 session 只需要开启一个协程
	s.start.Do(func() {
		//默认最大session数1000
		if int(incrSessions()) > s.config.ProxyMaxClients {
			go func() {
				s.Conn.Encode(redis.NewErrorf("ERR max number of clients reached"), true)
				s.CloseWithError(ErrTooManySessions)
				s.incrOpFails(nil, nil)
				s.flushOpStats(true)
			}()
			decrSessions()
			return
		}

		// todo 不在线是什么意思？什么时候会出现这种情况？
		if !d.isOnline() {
			go func() {
				s.Conn.Encode(redis.NewErrorf("ERR router is not online"), true)
				s.CloseWithError(ErrRouterNotOnline)
				s.incrOpFails(nil, nil)
				s.flushOpStats(true)
			}()
			decrSessions()
			return
		}

		//tasks是一个指向RequestChan的指针,RequestChan结构体中有一个data字段,
		//data字段是个数组，保存1024个指向Request的指针
		tasks := NewRequestChanBuffer(1024)

		go func() {
			//s.loopWriter是合并请求结果，针对mset/mget参数会有合并请求操作
			// 读取结果数据，并返回给用户
			s.loopWriter(tasks)
			decrSessions()
		}()

		go func() {
			//s.loopReader读取分发请求，首先根据key计算该key分配到哪个slot.
			//在此步骤中只会将slot对应的连接取出，然后将请求放到连接的input字段中。
			s.loopReader(tasks, d)
			tasks.Close()
		}()
	})
}

// 读取分发请求，首先根据key计算该key分配到哪个slot.在此步骤中只会将slot对应的连接取出，然后将请求放到连接的input字段中。
func (s *Session) loopReader(tasks *RequestChan, d *Router) (err error) {
	defer func() {
		s.CloseReaderWithError(err)
	}()

	var (
		breakOnFailure = s.config.SessionBreakOnFailure
		maxPipelineLen = s.config.SessionMaxPipeline
	)

	for !s.quit {
		// decode 用户请求，得到明文。比如：["GET","name"]
		// 这里面会阻塞，直到收到client发送的请求
		multi, err := s.Conn.DecodeMultiBulk()
		if err != nil {
			return err
		}
		if len(multi) == 0 {
			continue
		}
		s.incrOpTotal()

		// 如果当前正在排队处理的请求数量大于10000，则拒绝请求
		if tasks.Buffered() > maxPipelineLen {
			return s.incrOpFails(nil, ErrTooManyPipelinedRequests)
		}

		start := time.Now()
		s.LastOpUnix = start.Unix()
		s.Ops++

		//创建一个Request结构体，该结构体会贯穿请求的始终，请求字段，响应字段都放在Request中
		r := &Request{}
		r.Multi = multi
		r.Batch = &sync.WaitGroup{}
		//选择数据库，选择范围0 ～ 15。
		r.Database = s.database
		r.UnixNano = start.UnixNano()

		// 处理请求
		if err := s.handleRequest(r, d); err != nil {
			r.Resp = redis.NewErrorf("ERR handle request, %s", err)
			//如果handleRequest执行成功，将请求r放入tasks
			// handleRequest函数中的r放入tasks，即上文的RequestChan的data字段中。
			// loopWriter会从该字段中获取请求并且返回给客户端。
			tasks.PushBack(r)
			if breakOnFailure {
				return err
			}
		} else {
			tasks.PushBack(r)
		}
	}
	return nil
}

func (s *Session) loopWriter(tasks *RequestChan) (err error) {
	defer func() {
		s.CloseWithError(err)
		tasks.PopFrontAllVoid(func(r *Request) {
			s.incrOpFails(r, nil)
		})
		s.flushOpStats(true)
	}()

	var (
		breakOnFailure = s.config.SessionBreakOnFailure
		maxPipelineLen = s.config.SessionMaxPipeline
	)

	p := s.Conn.FlushEncoder()
	p.MaxInterval = time.Millisecond
	p.MaxBuffered = maxPipelineLen / 2

	return tasks.PopFrontAll(func(r *Request) error {
		resp, err := s.handleResponse(r) // r.*redis.Resp 即是执行结果
		if err != nil {
			resp = redis.NewErrorf("ERR handle response, %s", err)
			if breakOnFailure {
				s.Conn.Encode(resp, true)
				return s.incrOpFails(r, err)
			}
		}
		if err := p.Encode(resp); err != nil {
			return s.incrOpFails(r, err)
		}
		fflush := tasks.IsEmpty()
		if err := p.Flush(fflush); err != nil { // Flush 将执行结果推送给用户
			return s.incrOpFails(r, err)
		} else {
			s.incrOpStats(r, resp.Type) // 统计调用次数
		}
		if fflush {
			s.flushOpStats(false)
		}
		return nil
	})
}

func (s *Session) handleResponse(r *Request) (*redis.Resp, error) {
	r.Batch.Wait()
	if r.Coalesce != nil {
		if err := r.Coalesce(); err != nil {
			return nil, err
		}
	}
	if err := r.Err; err != nil {
		return nil, err
	} else if r.Resp == nil {
		return nil, ErrRespIsRequired
	}
	return r.Resp, nil
}

func (s *Session) handleRequest(r *Request, d *Router) error {
	opstr, flag, err := getOpInfo(r.Multi) // opstr = GET, flag = 0
	if err != nil {
		return err
	}
	r.OpStr = opstr
	r.OpFlag = flag
	r.Broken = &s.broken // 0

	if flag.IsNotAllowed() {
		return fmt.Errorf("command '%s' is not allowed", opstr)
	}

	switch opstr {
	case "QUIT":
		return s.handleQuit(r)
	case "AUTH":
		return s.handleAuth(r)
	}

	if !s.authorized {
		if s.config.SessionAuth != "" {
			r.Resp = redis.NewErrorf("NOAUTH Authentication required")
			return nil
		}
		s.authorized = true
	}

	switch opstr {
	case "SELECT":
		return s.handleSelect(r)
	case "PING":
		return s.handleRequestPing(r, d)
	case "INFO":
		return s.handleRequestInfo(r, d)
	case "MGET":
		return s.handleRequestMGet(r, d)
	case "MSET":
		return s.handleRequestMSet(r, d)
	case "DEL":
		return s.handleRequestDel(r, d)
	case "EXISTS":
		return s.handleRequestExists(r, d)
	case "SLOTSINFO":
		return s.handleRequestSlotsInfo(r, d)
	case "SLOTSSCAN":
		return s.handleRequestSlotsScan(r, d)
	case "SLOTSMAPPING":
		return s.handleRequestSlotsMapping(r, d)
	default:
		//分发slots函数
		return d.dispatch(r)
	}
}

func (s *Session) handleQuit(r *Request) error {
	s.quit = true
	r.Resp = RespOK
	return nil
}

func (s *Session) handleAuth(r *Request) error {
	if len(r.Multi) != 2 {
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'AUTH' command")
		return nil
	}
	switch {
	case s.config.SessionAuth == "":
		r.Resp = redis.NewErrorf("ERR Client sent AUTH, but no password is set")
	case s.config.SessionAuth != string(r.Multi[1].Value):
		s.authorized = false
		r.Resp = redis.NewErrorf("ERR invalid password")
	default:
		s.authorized = true
		r.Resp = RespOK
	}
	return nil
}

func (s *Session) handleSelect(r *Request) error {
	if len(r.Multi) != 2 {
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SELECT' command")
		return nil
	}
	switch db, err := strconv.Atoi(string(r.Multi[1].Value)); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR invalid DB index")
	case db < 0 || db >= int(s.config.BackendNumberDatabases):
		r.Resp = redis.NewErrorf("ERR invalid DB index, only accept DB [0,%d)", s.config.BackendNumberDatabases)
	default:
		r.Resp = RespOK
		s.database = int32(db)
	}
	return nil
}

func (s *Session) handleRequestPing(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0:
		slot := uint32(time.Now().Nanosecond()) % MaxSlotNum // 随机挑选slots进行处理
		return d.dispatchSlot(r, int(slot))
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestInfo(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0:
		slot := uint32(time.Now().Nanosecond()) % MaxSlotNum
		return d.dispatchSlot(r, int(slot))
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestMGet(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'MGET' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var array = make([]*redis.Resp, len(sub))
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsArray() && len(resp.Array) == 1:
				array[i] = resp.Array[0]
			default:
				return fmt.Errorf("bad mget resp: %s array.len = %d", resp.Type, len(resp.Array))
			}
		}
		r.Resp = redis.NewArray(array)
		return nil
	}
	return nil
}

func (s *Session) handleRequestMSet(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0 || nblks%2 != 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'MSET' command")
		return nil
	case nblks == 2:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nblks / 2)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i*2+1],
			r.Multi[i*2+2],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsString():
				r.Resp = resp
			default:
				return fmt.Errorf("bad mset resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		return nil
	}
	return nil
}

func (s *Session) handleRequestDel(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'DEL' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var n int
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsInt() && len(resp.Value) == 1:
				n += int(resp.Value[0] - '0')
			default:
				return fmt.Errorf("bad del resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		r.Resp = redis.NewInt(strconv.AppendInt(nil, int64(n), 10))
		return nil
	}
	return nil
}

func (s *Session) handleRequestExists(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'EXISTS' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var n int
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsInt() && len(resp.Value) == 1:
				if resp.Value[0] != '0' {
					n++
				}
			default:
				return fmt.Errorf("bad exists resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		r.Resp = redis.NewInt(strconv.AppendInt(nil, int64(n), 10))
		return nil
	}
	return nil
}

func (s *Session) handleRequestSlotsInfo(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks != 1:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSINFO' command")
		return nil
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestSlotsScan(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks <= 1:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSSCAN' command")
		return nil
	}
	switch slot, err := redis.Btoi64(r.Multi[1].Value); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, %s", r.Multi[1].Value, err)
		return nil
	case slot < 0 || slot >= MaxSlotNum:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, out of range", r.Multi[1].Value)
		return nil
	default:
		return d.dispatchSlot(r, int(slot))
	}
}

func (s *Session) handleRequestSlotsMapping(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks >= 2:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSMAPPING' command")
		return nil
	}
	marshalToResp := func(m *models.Slot) *redis.Resp {
		if m == nil {
			return redis.NewArray(nil)
		}
		var replicaGroups []*redis.Resp
		for i := range m.ReplicaGroups {
			var group []*redis.Resp
			for _, addr := range m.ReplicaGroups[i] {
				group = append(group, redis.NewString([]byte(addr)))
			}
			replicaGroups = append(replicaGroups, redis.NewArray(group))
		}
		return redis.NewArray([]*redis.Resp{
			redis.NewString([]byte(strconv.Itoa(m.Id))),
			redis.NewString([]byte(m.BackendAddr)),
			redis.NewString([]byte(m.MigrateFrom)),
			redis.NewArray(replicaGroups),
		})
	}
	if nblks == 0 {
		var array = make([]*redis.Resp, MaxSlotNum)
		for i, m := range d.GetSlots() {
			array[i] = marshalToResp(m)
		}
		r.Resp = redis.NewArray(array)
		return nil
	}
	switch slot, err := redis.Btoi64(r.Multi[1].Value); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, %s", r.Multi[1].Value, err)
		return nil
	case slot < 0 || slot >= MaxSlotNum:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, out of range", r.Multi[1].Value)
		return nil
	default:
		r.Resp = marshalToResp(d.GetSlot(int(slot)))
		return nil
	}
}

func (s *Session) incrOpTotal() {
	s.stats.total.Incr()
}

func (s *Session) getOpStats(opstr string) *opStats {
	e := s.stats.opmap[opstr]
	if e == nil {
		e = &opStats{opstr: opstr}
		s.stats.opmap[opstr] = e
	}
	return e
}

func (s *Session) incrOpStats(r *Request, t redis.RespType) {
	e := s.getOpStats(r.OpStr)
	e.calls.Incr()
	e.nsecs.Add(time.Now().UnixNano() - r.UnixNano)
	switch t {
	case redis.TypeError:
		e.redis.errors.Incr()
	}
}

func (s *Session) incrOpFails(r *Request, err error) error {
	if r != nil {
		e := s.getOpStats(r.OpStr)
		e.fails.Incr()
	} else {
		s.stats.fails.Incr()
	}
	return err
}

func (s *Session) flushOpStats(force bool) {
	var nano = time.Now().UnixNano()
	if !force {
		const period = int64(time.Millisecond) * 100
		if d := nano - s.stats.flush.nano; d < period {
			return
		}
	}
	s.stats.flush.nano = nano

	incrOpTotal(s.stats.total.Swap(0))
	incrOpFails(s.stats.fails.Swap(0))
	for _, e := range s.stats.opmap {
		if e.calls.Int64() != 0 || e.fails.Int64() != 0 {
			incrOpStats(e)
		}
	}
	s.stats.flush.n++

	if len(s.stats.opmap) <= 32 {
		return
	}
	if (s.stats.flush.n % 16384) == 0 {
		s.stats.opmap = make(map[string]*opStats, 32)
	}
}
