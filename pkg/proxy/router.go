// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
)

//const MaxSlotNum = models.MaxSlotNum

type Router struct {
	mu sync.RWMutex
	tableMutex sync.RWMutex

	pool struct {
		primary *sharedBackendConnPool
		replica *sharedBackendConnPool
	}
	slots map[int][]Slot
	table map[int]*models.Table

	config *Config
	online bool
	closed bool
}

func NewRouter(config *Config) *Router {
	s := &Router{config: config}
	s.pool.primary = newSharedBackendConnPool(config, config.BackendPrimaryParallel)
	s.pool.replica = newSharedBackendConnPool(config, config.BackendReplicaParallel)
	s.table = make(map[int]*models.Table)
	s.slots = make(map[int][]Slot)
//	for i := range s.slots {
//		s.slots[i].id = i
//		s.slots[i].method = &forwardSync{}
//	}
	return s
}

func (s *Router) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.online = true
}

func (s *Router) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true

	for i := range s.slots {
		s.fillSlot(&models.Slot{Id: i}, false, nil)
	}
}

func (s *Router) GetSlots() []*models.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var slots []*models.Slot
	for i := range s.slots {
		for j := range s.slots[i] {
			slots = append(slots, s.slots[i][j].snapshot())
		}
	}
	return slots
}

func (s *Router) GetSlot(tid, sid int) *models.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sid < 0 || sid >= s.table[tid].MaxSlotMum {
		return nil
	}
	slot := &s.slots[tid][sid]
	return slot.snapshot()
}

func (s *Router) HasSwitched() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, i := range s.slots {
		for _, j := range i {
			if j.switched {
				return true
			}
		}
	}
	return false
}

var (
	ErrClosedRouter  = errors.New("use of closed router")
	ErrInvalidSlotId = errors.New("use of invalid slot id")
	ErrInvalidTableId = errors.New("use of invalid table id")
	ErrInvalidMethod = errors.New("use of invalid forwarder method")
)


func (s *Router) DelTable(t *models.Table) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	delete(s.table, t.Id)
}

func (s *Router) FillTable(t *models.Table) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.table[t.Id] = t
	log.Infof("fill table-[%d]", t.Id)
}

func (s *Router) GetTable(tid int) *models.Table {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t , ok := s.table[tid]; ok {
		return t
	}
	return nil
}

func (s *Router) FillSlot(m *models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedRouter
	}
	//s.tableMutex.RLock()
	//defer s.tableMutex.RUnlock()
	if _, ok := s.table[m.TableId]; !ok {
		log.Infof("slot table id: %d, slot id: %d ", m.TableId, m.Id)
		return  ErrInvalidTableId
	}
	if m.Id < 0 || m.Id >= s.table[m.TableId].MaxSlotMum {
		return ErrInvalidSlotId
	}
	var method forwardMethod
	switch m.ForwardMethod {
	default:
		return ErrInvalidMethod
	case models.ForwardSync:
		method = &forwardSync{}
	case models.ForwardSemiAsync:
		method = &forwardSemiAsync{}
	}
	s.fillSlot(m, false, method)
	return nil
}

func (s *Router) KeepAlive() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosedRouter
	}
	s.pool.primary.KeepAlive()
	s.pool.replica.KeepAlive()
	return nil
}

func (s *Router) isOnline() bool {
	return s.online && !s.closed
}

func (s *Router) dispatch(r *Request) error {
	hkey := getHashKey(r.Multi, r.OpStr)
	var id = Hash(hkey) % uint32(s.table[r.Database].MaxSlotMum)
	slot := &s.slots[r.Database][id]
	return slot.forward(r, hkey)
}

func (s *Router) dispatchSlot(r *Request, id int) error {
	if id < 0 || id >= s.table[r.Database].MaxSlotMum {
		return ErrInvalidSlotId
	}
	slot := &s.slots[r.Database][id]
	return slot.forward(r, nil)
}

func (s *Router) dispatchAddr(r *Request, addr string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if bc := s.pool.primary.Get(addr).BackendConn(r.Database, r.Seed16(), false); bc != nil {
		bc.PushBack(r)
		return true
	}
	if bc := s.pool.replica.Get(addr).BackendConn(r.Database, r.Seed16(), false); bc != nil {
		bc.PushBack(r)
		return true
	}
	return false
}

func (s *Router) fillSlot(m *models.Slot, switched bool, method forwardMethod) {
	if _ , ok := s.slots[m.TableId]; !ok {
		slots := make ([]Slot, s.table[m.TableId].MaxSlotMum)
		s.slots[m.TableId] = slots
		for i := range slots {
			slots[i].id = i
			slots[i].method = &forwardSync{}
		}
	}
	slot := &s.slots[m.TableId][m.Id]
	slot.blockAndWait()

	slot.backend.bc.Release(m.TableId)
	slot.backend.bc = nil
	slot.backend.id = 0
	slot.migrate.bc.Release(m.TableId)
	slot.migrate.bc = nil
	slot.migrate.id = 0
	for i := range slot.replicaGroups {
		for _, bc := range slot.replicaGroups[i] {
			bc.Release(m.TableId)
		}
	}
	slot.replicaGroups = nil

	slot.switched = switched

	if addr := m.BackendAddr; len(addr) != 0 {
		slot.backend.bc = s.pool.primary.Retain(s.table[m.TableId],  addr)
		slot.backend.id = m.BackendAddrGroupId
	}
	if from := m.MigrateFrom; len(from) != 0 {
		slot.migrate.bc = s.pool.primary.Retain(s.table[m.TableId], from)
		slot.migrate.id = m.MigrateFromGroupId
	}
	if !s.config.BackendPrimaryOnly {
		for i := range m.ReplicaGroups {
			var group []*sharedBackendConn
			for _, addr := range m.ReplicaGroups[i] {
				group = append(group, s.pool.replica.Retain(s.table[m.TableId], addr))
			}
			if len(group) == 0 {
				continue
			}
			slot.replicaGroups = append(slot.replicaGroups, group)
		}
	}
	if method != nil {
		slot.method = method
	}

	if !m.Locked {
		slot.unblock()
	}
	if !s.closed {
		if slot.migrate.bc != nil {
			if switched {
				log.Warnf("fill slot %04d, backend.addr = %s, migrate.from = %s, locked = %t, +switched",
					slot.id, slot.backend.bc.Addr(), slot.migrate.bc.Addr(), slot.lock.hold)
			} else {
				log.Warnf("fill slot %04d, backend.addr = %s, migrate.from = %s, locked = %t",
					slot.id, slot.backend.bc.Addr(), slot.migrate.bc.Addr(), slot.lock.hold)
			}
		} else {
			if switched {
				log.Warnf("fill slot %04d, backend.addr = %s, locked = %t, +switched",
					slot.id, slot.backend.bc.Addr(), slot.lock.hold)
			} else {
				log.Warnf("fill slot %04d, table %04d, backend.addr = %s, locked = %t",
					slot.id, m.TableId, slot.backend.bc.Addr(), slot.lock.hold)
			}
		}
	}
}

func (s *Router) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedRouter
	}
	cache := &redis.InfoCache{
		Auth: s.config.ProductAuth, Timeout: time.Millisecond * 100,
	}
	for i := range s.slots {
		for j := range(s.slots[i])  {
			s.trySwitchMaster(i, j, masters, cache)
		}
	}
	return nil
}

func (s *Router) trySwitchMaster(tid, sid int, masters map[int]string, cache *redis.InfoCache) {
	var switched bool
	var m = s.slots[tid][sid].snapshot()

	hasSameRunId := func(addr1, addr2 string) bool {
		if addr1 != addr2 {
			rid1 := cache.GetRunId(addr1)
			rid2 := cache.GetRunId(addr2)
			return rid1 != "" && rid1 == rid2
		}
		return true
	}

	if addr := masters[m.BackendAddrGroupId]; addr != "" {
		if !hasSameRunId(addr, m.BackendAddr) {
			m.BackendAddr, switched = addr, true
		}
	}
	if addr := masters[m.MigrateFromGroupId]; addr != "" {
		if !hasSameRunId(addr, m.MigrateFrom) {
			m.MigrateFrom, switched = addr, true
		}
	}
	if switched {
		s.fillSlot(m, true, nil)
	}
}
