package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
	"time"
)

const  donwAfterPeriod = 10000
const  infoPeriod = 1000
const  pingPeriod = 1000

type PikaInfo struct {
	Table map[int]*redis.InfoTable
	Error *rpc.RemoteError  `json:"error,omitempty"`
	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

type PikaPing struct {
	Addr     string
	Gid      int
	ServerId int
	Offline  bool
	Error    *rpc.RemoteError  `json:"error,omitempty"`
	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

type Server struct {
	Status *PikaPing
	FailTime int64
}

type Offline struct {
	Servers [] *Server
	Action string
}

type Slave struct {
	Addr string
	Master int
}

func (s *Topom) SetManager(status bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.managerOn = status
}

func (s *Topom) SetManagerOff() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.managerOn = false
}

func (s *Topom) GetManager() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.managerOn
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
			ping.Offline = true
			ping.Error = rpc.NewRemoteError(err)
		} else {
			ping.Addr = addr
			ping.Offline = p.Offline
		}
	}()

	select {
	case <-ch:
		return ping
	case <-time.After(timeout):
		return &PikaPing{Timeout: true, Offline: true, Addr: addr}
	}
}

func (s *Topom) HandleInfo (){

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
	for gid, g := range ctx.group {
		for sid, x := range g.Servers {
			goPing(x.Addr, func(addr string) (*PikaPing, error) {
				m, err := s.manager.redisp.Ping(addr)
				if err != nil {
					return nil, err
				}
				return &PikaPing{Offline: m, Gid: gid, ServerId: sid}, nil
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

func (s *Topom) Manager() {
	down := make(chan int)
	defer close(down)
	go s.PingServer(pingPeriod, down)
	go s.InfoServer(infoPeriod)
	for _ = range down {
		if s.GetManager() == false {
			break
		}
		s.HandleOffline()
	}
}

func (s *Topom) PingServer(interval time.Duration, down chan int)  {
	for s.GetManager() == true {
		w, err := s.PikaPing(time.Second)
		if err != nil {
			log.Warnf("check server Offline error: %s", err)
		}
		if w != nil {
			w.Wait()
		}
		down <- 1
		time.Sleep(time.Millisecond * interval)
	}
}

func (s *Topom) InfoServer(interval time.Duration)  {
	for s.GetManager() == true {
		w, err := s.RefreshPikaInfo(time.Second)
		if err != nil {
			log.Warnf("check server Offline error: %s", err)
		}
		if w != nil {
			w.Wait()
		}
		time.Sleep(time.Millisecond * interval)
	}
}

func (s *Topom) HandleOffline() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return  err
	}
	for _, p := range s.manager.status {
		if p.Offline == true {
			if _, ok := s.manager.offLine[p.Gid]; ok == false {
				var servers []*Server
				s.manager.offLine[p.Gid].Servers = servers
			}
			downServer := s.manager.offLine[p.Gid].Servers[p.ServerId]
			if downServer == nil {
				s.manager.offLine[p.Gid].Servers[p.ServerId] = &Server{FailTime: p.UnixTime, Status: p}
				break
			}
			if s.manager.offLine[p.Gid].Action == models.ActionNothing {
				if p.UnixTime - s.manager.offLine[p.Gid].Servers[p.ServerId].FailTime > donwAfterPeriod {
					if ctx.group[p.Gid].Servers[0].Addr == p.Addr {
						s.manager.offLine[p.Gid].Action = models.ActionPreparing
						go s.HandleMigrate(p.Gid)
					}
				}
			}
		} else {
			if _, ok := s.manager.offLine[p.Gid]; ok == false {
				break
			}
			if s.manager.offLine[p.Gid].Servers[p.ServerId] == nil {
				break
			}
			if s.manager.offLine[p.Gid].Action == models.ActionNothing {
				s.manager.offLine[p.Gid].Servers[p.ServerId] = nil
			}
		}
	}
	return nil
}

func (s *Topom) SelectSlave(gid int) error {
	return  nil
}

func (s *Topom) HandleMigrate(gid int) error {
	s.mu.Lock()
	ctx, err := s.newContext()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	//pikaInfo := make(map[string]*PikaInfo)
	pikaInfo := s.manager.servers
	s.mu.Unlock()
	if (len(g.Servers) - 1) == 0 {
		return nil
	}

	slaves := make(map[int]Slave, len(g.Servers) - 1)
	for i, _ := range g.Servers[1:] {
		if pikaInfo[g.Servers[i].Addr].Timeout == true {
			break
		}
		slaves[i] = Slave{Addr: g.Servers[i].Addr}
	}
	masterLog := make(map[int]redis.InfoTable)
	for _, slave := range slaves {
		for  t := range pikaInfo[slave.Addr].Table {
			if _, ok := masterLog[t]; ok == false {
				slot := make(map[int]*redis.InfoSlot)
				masterLog[t] = redis.InfoTable{Slot: slot}
			}
			for j := range pikaInfo[slave.Addr].Table[t].Slot {
				if _, ok :=masterLog[t].Slot[j]; ok ==false{
					masterLog[t].Slot[j] = pikaInfo[slave.Addr].Table[t].Slot[j]
					masterLog[t].Slot[j].MasterAddr = slave.Addr
					break
				}
				if pikaInfo[slave.Addr].Table[t].Slot[j].Consensus == false {
					if pikaInfo[slave.Addr].Table[t].Slot[j].FileNum > masterLog[t].Slot[j].FileNum {
						masterLog[t].Slot[j] = pikaInfo[slave.Addr].Table[t].Slot[j]
						masterLog[t].Slot[j].MasterAddr = slave.Addr
						break
					}
					if pikaInfo[slave.Addr].Table[t].Slot[j].FileNum == masterLog[t].Slot[j].FileNum && pikaInfo[slave.Addr].Table[t].Slot[j].Offset > masterLog[t].Slot[j].Offset {
						masterLog[t].Slot[j] = pikaInfo[slave.Addr].Table[t].Slot[j]
						masterLog[t].Slot[j].MasterAddr = slave.Addr
					}
				} else {
					if pikaInfo[slave.Addr].Table[t].Slot[j].Term > masterLog[t].Slot[j].Term {
						masterLog[t].Slot[j] = pikaInfo[slave.Addr].Table[t].Slot[j]
						masterLog[t].Slot[j].MasterAddr = slave.Addr
						break
					}
					if pikaInfo[slave.Addr].Table[t].Slot[j].Term == masterLog[t].Slot[j].Term && pikaInfo[slave.Addr].Table[t].Slot[j].Index > masterLog[t].Slot[j].Index {
						masterLog[t].Slot[j] = pikaInfo[slave.Addr].Table[t].Slot[j]
						masterLog[t].Slot[j].MasterAddr = slave.Addr
					}

				}
			}
		}
	}
	for _, t := range masterLog {
		for _, slot := range t.Slot {
			for _, slave := range slaves {
				if slot.MasterAddr == slave.Addr {
					slave.Master++
				}
			}
		}
	}
	var max, master int
	for i, slave := range slaves {
		if slave.Master > max {
			max = slave.Master
			master = i
		}
	}
	for i, t := range pikaInfo[slaves[master].Addr].Table {
		if err := s.manager.redisp.SlveofNoOne(slaves[master].Addr, i); err !=nil {
			log.Warnf("group-[%d] addr-[%s] slaveof no one error:%s", gid, slaves[master].Addr, err)
		}
		for j, slot := range t.Slot {
			if slot.MasterAddr != masterLog[i].Slot[j].MasterAddr {
				if err := s.manager.redisp.SlotSlaveof(slaves[master].Addr, j, i); err !=nil {
					log.Warnf("group-[%d] addr-[%s] slotsslaveof slot-[%d] table-[%d] error:%s", gid, slaves[master].Addr, j, i, err)
				}
			}
		}
	}
	var done bool
	for s.IsOnline(){
		time.Sleep(time.Second)
		s.mu.Lock()
		for i, t := range s.manager.servers[slaves[master].Addr].Table {
			for j, slot := range t.Slot {
				if slot.FileNum != pikaInfo[slaves[master].Addr].Table[i].Slot[j].FileNum {
					done = false
					break
				}
				if slot.Offset != pikaInfo[slaves[master].Addr].Table[i].Slot[j].Offset {
					done = false
					break
				}
			}
		}
		s.mu.Unlock()
		if done == true {
			for _, slave := range slaves {
				if slaves[master].Addr != slave.Addr {
					for  t := range pikaInfo[slave.Addr].Table {
						if err := s.manager.redisp.SlveofNoOne(slave.Addr, t); err != nil {
							log.Warnf("group-[%d] addr-[%s] slaveof no one error:%s", gid, slaves[master].Addr, err)
							break
						}
						if err := s.manager.redisp.SlotSlaveofAll(slave.Addr, t); err !=nil {
							log.Warnf("group-[%d] addr-[%s] slotsslaveof all table-[%d] error:%s", gid, slaves[master].Addr, t, err)
						}
					}
				}
			}
			break
		}
	}
	s.GroupPromoteServer(gid, slaves[master].Addr)
	s.mu.Lock()
	s.manager.offLine[gid].Action = models.ActionNothing
	s.mu.Unlock()
	return  nil
}

