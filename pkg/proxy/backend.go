// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

const (
	stateConnected = iota + 1
	stateDataStale
)

type BackendConn struct {
	stop sync.Once
	addr string // 127.0.0.1:6379

	//buffer为1024的channel
	// 每当有请求进来，都会放到input中，然后遍历进行消费处理。参考pushBack方法的逻辑
	input chan *Request
	retry struct {
		fails int
		delay Delay
	}
	state atomic2.Int64 // 1

	closed atomic2.Bool // 0
	config *Config

	database int // 0
}

func NewBackendConn(addr string, database int, config *Config) *BackendConn {
	bc := &BackendConn{
		addr: addr, config: config, database: database,
	}
	bc.input = make(chan *Request, 1024)
	bc.retry.delay = &DelayExp2{
		Min: 50, Max: 5000,
		Unit: time.Millisecond,
	}

	go bc.run()

	return bc
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
	bc.closed.Set(true)
}

func (bc *BackendConn) IsConnected() bool {
	return bc.state.Int64() == stateConnected
}

//请求放入BackendConn等待处理。如果request的sync.WaitGroup不为空，就加一，然后判断加一之后的值，如果加一之后couter为0，那么所有阻塞在counter上的goroutine都会得到释放
//将请求直接存入到BackendConn的chan *Request中，等待后续被取出并进行处理。
func (bc *BackendConn) PushBack(r *Request) {
	if r.Batch != nil {
		//请求处理之后，setResponse的时候，r.Batch会减1，表明请求已经处理完。
		//session的loopWriter里面收集请求结果的时候，会调用wait方法等一次的所有请求处理完成
		r.Batch.Add(1)
	}
	//将请求放入BackendConn的input channel
	bc.input <- r
}

func (bc *BackendConn) KeepAlive() bool {
	if len(bc.input) != 0 {
		return false
	}
	switch bc.state.Int64() {
	default:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("PING")),
		}
		bc.PushBack(m)

	case stateDataStale:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("INFO")),
		}
		m.Batch = &sync.WaitGroup{}
		bc.PushBack(m)

		keepAliveCallback <- func() {
			m.Batch.Wait()
			var err = func() error {
				if err := m.Err; err != nil {
					return err
				}
				switch resp := m.Resp; {
				case resp == nil:
					return ErrRespIsRequired
				case resp.IsError():
					return fmt.Errorf("bad info resp: %s", resp.Value)
				case resp.IsBulkBytes():
					var info = make(map[string]string)
					for _, line := range strings.Split(string(resp.Value), "\n") {
						kv := strings.SplitN(line, ":", 2)
						if len(kv) != 2 {
							continue
						}
						if key := strings.TrimSpace(kv[0]); key != "" {
							info[key] = strings.TrimSpace(kv[1])
						}
					}
					if info["master_link_status"] == "down" {
						return nil
					}
					if info["loading"] == "1" {
						return nil
					}
					if bc.state.CompareAndSwap(stateDataStale, stateConnected) {
						log.Warnf("backend conn [%p] to %s, db-%d state = Connected (keepalive)",
							bc, bc.addr, bc.database)
					}
					return nil
				default:
					return fmt.Errorf("bad info resp: should be string, but got %s", resp.Type)
				}
			}()
			if err != nil && bc.closed.IsFalse() {
				log.WarnErrorf(err, "backend conn [%p] to %s, db-%d recover from DataStale failed",
					bc, bc.addr, bc.database)
			}
		}
	}
	return true
}

var keepAliveCallback = make(chan func(), 128)

func init() {
	go func() {
		for fn := range keepAliveCallback {
			fn()
		}
	}()
}

func (bc *BackendConn) newBackendReader(round int, config *Config) (*redis.Conn, chan<- *Request, error) {
	c, err := redis.DialTimeout(bc.addr, time.Second*5, // bc.addr = 127.0.0.1:6379
		config.BackendRecvBufsize.AsInt(),
		config.BackendSendBufsize.AsInt())
	if err != nil {
		return nil, nil, err
	}
	c.ReaderTimeout = config.BackendRecvTimeout.Duration()
	c.WriterTimeout = config.BackendSendTimeout.Duration()
	c.SetKeepAlivePeriod(config.BackendKeepAlivePeriod.Duration())

	if err := bc.verifyAuth(c, config.ProductAuth); err != nil {
		c.Close()
		return nil, nil, err
	}
	if err := bc.selectDatabase(c, bc.database); err != nil {
		c.Close()
		return nil, nil, err
	}

	tasks := make(chan *Request, config.BackendMaxPipeline)
	go bc.loopReader(tasks, c, round) // 将task转发到redis中去执行

	return c, tasks, nil
}

func (bc *BackendConn) verifyAuth(c *redis.Conn, auth string) error {
	if auth == "" {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("AUTH")),
		redis.NewBulkBytes([]byte(auth)),
	}

	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

func (bc *BackendConn) selectDatabase(c *redis.Conn, database int) error {
	if database == 0 {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("SELECT")),
		redis.NewBulkBytes([]byte(strconv.Itoa(database))),
	}

	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	val := strings.Compare(string(resp.Value), "PONG") == 0
	_ = val
	r.Resp, r.Err = resp, err
	if r.Group != nil {
		r.Group.Done()
	}
	if r.Batch != nil {
		r.Batch.Done()
	}
	return err
}

var (
	ErrBackendConnReset = errors.New("backend conn reset")
	ErrRequestIsBroken  = errors.New("request is broken")
)

func (bc *BackendConn) run() {
	log.Warnf("backend conn [%p] to %s, db-%d start service",
		bc, bc.addr, bc.database)
	for round := 0; bc.closed.IsFalse(); round++ {
		log.Warnf("backend conn [%p] to %s, db-%d round-[%d]",
			bc, bc.addr, bc.database, round)
		if err := bc.loopWriter(round); err != nil {
			bc.delayBeforeRetry()
		}
	}
	log.Warnf("backend conn [%p] to %s, db-%d stop and exit",
		bc, bc.addr, bc.database)
}

var (
	errRespMasterDown = []byte("MASTERDOWN")
	errRespLoading    = []byte("LOADING")
)

func (bc *BackendConn) loopReader(tasks <-chan *Request, c *redis.Conn, round int) (err error) {
	defer func() {
		c.Close()
		for r := range tasks {
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d reader-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()
	for r := range tasks { //获取结果，并返回给client端
		resp, err := c.Decode()
		//cc := strings.Compare(string(resp.Value), "PONG") == 0
		//_ = cc
		if err != nil { // 这里接收所有的请求，比如 PING 等等
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		if resp != nil && resp.IsError() {
			switch {
			case bytes.HasPrefix(resp.Value, errRespMasterDown):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'MASTERDOWN'",
						bc, bc.addr, bc.database)
				}
			case bytes.HasPrefix(resp.Value, errRespLoading):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'LOADING'",
						bc, bc.addr, bc.database)
				}
			}
		}
		bc.setResponse(r, resp, nil)
	}
	return nil
}

func (bc *BackendConn) delayBeforeRetry() {
	bc.retry.fails += 1
	if bc.retry.fails <= 10 {
		return
	}
	timeout := bc.retry.delay.After()
	for bc.closed.IsFalse() {
		select {
		case <-timeout:
			return
		case r, ok := <-bc.input:
			if !ok {
				return
			}
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
	}
}

func (bc *BackendConn) loopWriter(round int) (err error) {
	defer func() {
		for i := len(bc.input); i != 0; i-- {
			r := <-bc.input // 如果程序结束，把未来得及处理的请求，依次返回错误码
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d writer-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()
	// 调用newBackendReader函数。注意此处的tasks也是一个存放*Request的channel,用来此处的loopWriter和loopReader交流信息
	c, tasks, err := bc.newBackendReader(round, bc.config)
	if err != nil {
		return err
	}
	defer close(tasks)

	defer bc.state.Set(0)

	bc.state.Set(stateConnected)
	bc.retry.fails = 0
	bc.retry.delay.Reset()

	p := c.FlushEncoder()
	p.MaxInterval = time.Millisecond
	p.MaxBuffered = cap(tasks) / 2

	// 可以看到,此处的loopWriter会从bc.input中取出数据并且处理
	for r := range bc.input {
		if r.IsReadOnly() && r.IsBroken() {
			bc.setResponse(r, nil, ErrRequestIsBroken)
			continue
		}
		//将请求编码并且发送到codis server
		if err := p.EncodeMultiBulk(r.Multi); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		// 将请求推送给redis-server执行
		if err := p.Flush(len(bc.input) == 0); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		} else {
			//opt := strings.Compare(r.OpStr, "GET") == 0
			//_ = opt
			//将请求放入tasks这个channel中
			tasks <- r
		}
	}
	return nil
}

type sharedBackendConn struct { // sharedBackendConn 负责对redis 的请求进行处理
	addr string //codis server的地址，127.0.0.1:6379
	host []byte //codis server的主机名， 127.0.0.1
	port []byte //codis server的端口，6379

	owner *sharedBackendConnPool //属于哪个连接池
	conns [][]*BackendConn       //二维数组,一般codis server会有16个db,第一个维度为0-15的数组,每个db可以有多个BackendConn连接

	single []*BackendConn //如果每个db只有一个BackendConn连接，则直接放入single中。当每个db有多个连接时会从conns中选一个返回，而每个db只有一个连接时，直接从single中返回

	// 当前sharedBackendConn的引用计数，非正数的时候表明关闭。每多一个引用就加一
	refcnt int
}

func newSharedBackendConn(addr string, pool *sharedBackendConnPool) *sharedBackendConn {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.ErrorErrorf(err, "split host-port failed, address = %s", addr)
	}
	s := &sharedBackendConn{
		addr: addr,
		host: []byte(host), port: []byte(port),
	}
	s.owner = pool
	s.conns = make([][]*BackendConn, pool.config.BackendNumberDatabases)
	for database := range s.conns {
		parallel := make([]*BackendConn, pool.parallel)
		for i := range parallel {
			parallel[i] = NewBackendConn(addr, database, pool.config)
		}
		s.conns[database] = parallel
	}
	if pool.parallel == 1 {
		s.single = make([]*BackendConn, len(s.conns))
		for database := range s.conns {
			s.single[database] = s.conns[database][0]
		}
	}
	s.refcnt = 1
	return s
}

func (s *sharedBackendConn) Addr() string {
	if s == nil {
		return ""
	}
	return s.addr
}

func (s *sharedBackendConn) Release() {
	if s == nil {
		return
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	} else {
		s.refcnt--
	}
	if s.refcnt != 0 {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.Close()
		}
	}
	delete(s.owner.pool, s.addr)
}

func (s *sharedBackendConn) Retain() *sharedBackendConn {
	if s == nil {
		return nil
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed")
	} else {
		s.refcnt++
	}
	return s
}

func (s *sharedBackendConn) KeepAlive() {
	if s == nil {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.KeepAlive()
		}
	}
}

// 如果single不为空，说明每一个DB只有一个连接，直接取出来使用
// 否则表示每个DB可能有多个链接，则随机去一个可用的连接来使用
func (s *sharedBackendConn) BackendConn(database int32, seed uint, must bool) *BackendConn {
	if s == nil {
		return nil
	}

	if s.single != nil {
		bc := s.single[database]
		if must || bc.IsConnected() {
			return bc
		}
		return nil
	}

	var parallel = s.conns[database]

	var i = seed
	for range parallel {
		i = (i + 1) % uint(len(parallel))
		if bc := parallel[i]; bc.IsConnected() {
			return bc
		}
	}
	if !must {
		return nil
	}
	return parallel[0]
}

type sharedBackendConnPool struct {
	config   *Config
	parallel int // 1

	pool map[string]*sharedBackendConn
}

func newSharedBackendConnPool(config *Config, parallel int) *sharedBackendConnPool {
	p := &sharedBackendConnPool{
		config: config, parallel: math2.MaxInt(1, parallel),
	}
	p.pool = make(map[string]*sharedBackendConn)
	return p
}

func (p *sharedBackendConnPool) KeepAlive() {
	for _, bc := range p.pool {
		bc.KeepAlive()
	}
}

func (p *sharedBackendConnPool) Get(addr string) *sharedBackendConn {
	return p.pool[addr]
}

func (p *sharedBackendConnPool) Retain(addr string) *sharedBackendConn {
	if bc := p.pool[addr]; bc != nil {
		return bc.Retain()
	} else {
		bc = newSharedBackendConn(addr, p)
		p.pool[addr] = bc
		return bc
	}
}
