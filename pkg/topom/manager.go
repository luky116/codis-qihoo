package topom

import (
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
	"time"
)

const  donwAfterPeriod = 1000
const  infoPeriod = 10000

type PikaInfo struct {
	Table map[int]redis.InfoTable
//	Stats map[string]string `json:"stats,omitempty"`
	Error *rpc.RemoteError  `json:"error,omitempty"`

	Sentinel map[string]*redis.SentinelGroup `json:"sentinel,omitempty"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

type PikaPing struct {
	Addr string
	online bool
	Error *rpc.RemoteError  `json:"error,omitempty"`
	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) RefreshPikaInfo(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	goInfo := func(addr string, do func(addr string) (*PikaInfo, error)) {
		fut.Add()
		go func() {
			info := s.newPikaInfo(addr, timeout, do)
			info.UnixTime = time.Now().Unix()
			fut.Done(addr, info)
		}()
	}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			goInfo(x.Addr, func(addr string) (*PikaInfo, error) {
				m, err := s.manager.redisp.InfoSlot(addr)
				if err != nil {
					return nil, err
				}
				return &PikaInfo{Table: m}, nil
			})
		}
	}
	go func() {
		info := make(map[string]*PikaInfo)
		for k, v := range fut.Wait() {
			info[k] = v.(*PikaInfo)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.manager.servers = info
	}()
	return &fut, nil
}

func (s *Topom) newPikaInfo(addr string, timeout time.Duration, do func(addr string) (*PikaInfo, error)) *PikaInfo {
	var ch = make(chan struct{})
	info:= &PikaInfo{}

	go func() {
		defer close(ch)
		p, err := do(addr)
		if err != nil {
			info.Error = rpc.NewRemoteError(err)
		} else {
			info.Table = p.Table
		}
	}()

	select {
	case <-ch:
		return info
	case <-time.After(timeout):
		return &PikaInfo{Timeout: true}
	}
}

func (s *Topom) newPikaPing(addr string, timeout time.Duration, do func(addr string) (* PikaPing, error)) *PikaPing {
	var ch = make(chan struct{})
	ping:= &PikaPing{}

	go func() {
		defer close(ch)
		p, err := do(addr)
		if err != nil {
			ping.online = false
			ping.Error = rpc.NewRemoteError(err)
		} else {
			ping.Addr = addr
			ping.online = p.online
		}
	}()

	select {
	case <-ch:
		return ping
	case <-time.After(timeout):
		return &PikaPing{Timeout: true, online: false}
	}
}

func (s *Topom) HandleInfo (){

}

func (s *Topom) GetMaster () (map[int]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	master := make(map[int]string)
	for i, g := range ctx.group {
		master[i] = g.Servers[0].Addr
	}
	return master, nil
}

func (s *Topom) PikaPing(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	goPing := func(addr string, do func(addr string) (*PikaPing, error)) {
		fut.Add()
		go func() {
			ping := s.newPikaPing(addr, timeout, do)
			ping.UnixTime = time.Now().Unix()
			fut.Done(addr, ping)
		}()
	}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			goPing(x.Addr, func(addr string) (*PikaPing, error) {
				m, err := s.manager.redisp.Ping(addr)
				if err != nil {
					return nil, err
				}
				return &PikaPing{online: m}, nil
			})
		}
	}
	go func() {
		status := make(map[string]*PikaPing)
		for k, v := range fut.Wait() {
			status[k] = v.(*PikaPing)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.manager.status = status
	}()
	return &fut, nil
}

func (* Topom) Manager() error {

}
