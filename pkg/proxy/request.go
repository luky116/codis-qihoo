// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	"sync"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Request struct {
	Multi []*redis.Resp
	Batch *sync.WaitGroup
	Group *sync.WaitGroup

	Broken *atomic2.Bool

	OpStr string
	OpFlag

	Database int
	UnixNano int64

	*redis.Resp
	Err error

	Coalesce func() error
	Disassembly []int
}

func (r *Request) IsBroken() bool {
	return r.Broken != nil && r.Broken.IsTrue()
}

func (r *Request) MakeSubRequest(n int) []Request {
	var sub = make([]Request, n)
	for i := range sub {
		x := &sub[i]
		x.Batch = r.Batch
		x.OpStr = r.OpStr
		x.OpFlag = r.OpFlag
		x.Broken = r.Broken
		x.Database = r.Database
		x.UnixNano = r.UnixNano
	}
	return sub
}

func (s *Request) splite(d * Router) ([]* Request, []* Request, error) {
	var sub []* Request
	var mgr []* Request
	slotNum, err := d.getSlotNum(s.Database)
	if err != nil {
		return nil, nil, err
	}
	for i, key := range s.Multi[1:] {
		var id = Hash(key.Value) % uint32(slotNum)
		slot := &d.slots[s.Database][id]
		if slot.migrate.bc != nil {
			var disassemble []int
			disassemble = append(disassemble, i)
			request := &Request{
				Batch:       s.Batch,
				Broken:      s.Broken,
				OpStr:       s.OpStr,
				OpFlag:      s.OpFlag,
				Database:    s.Database,
				UnixNano:    s.UnixNano,
				Disassembly: disassemble,
			}
			request.Multi = []*redis.Resp{
				s.Multi[0],
				s.Multi[i+1],
			}
			mgr = append(mgr, request)
		} else {
			if slot.backend.bc == nil {
				log.Debugf("slot-%04d is not ready: hash key = '%s'",
					slot.id, key.Value)
				return nil, nil,  ErrSlotIsNotReady
			} else {
				gid := slot.backend.id
				if sub[gid] != nil {
					var disassemble []int
					disassemble = append(disassemble, i)
					request := &Request{
						Batch:       s.Batch,
						Broken:      s.Broken,
						OpStr:       s.OpStr,
						OpFlag:      s.OpFlag,
						Database:    s.Database,
						UnixNano:    s.UnixNano,
						Disassembly: disassemble,
					}
					request.Multi = []*redis.Resp{
						s.Multi[0],
						s.Multi[i+1],
					}
					sub[gid] = request
				} else {
					sub[gid].Multi = append(sub[gid].Multi, key)
					sub[gid].Disassembly = append(sub[gid].Disassembly, i)
				}
			}
		}
	}
	return sub, mgr, nil
}

const GOLDEN_RATIO_PRIME_32 = 0x9e370001

func (r *Request) Seed16() uint {
	h32 := uint32(r.UnixNano) + uint32(uintptr(unsafe.Pointer(r)))
	h32 *= GOLDEN_RATIO_PRIME_32
	return uint(h32 >> 16)
}

type RequestChan struct {
	lock sync.Mutex
	cond *sync.Cond

	data []*Request
	buff []*Request

	waits  int
	closed bool
}

const DefaultRequestChanBuffer = 128

func NewRequestChan() *RequestChan {
	return NewRequestChanBuffer(0)
}

func NewRequestChanBuffer(n int) *RequestChan {
	if n <= 0 {
		n = DefaultRequestChanBuffer
	}
	var ch = &RequestChan{
		buff: make([]*Request, n),
	}
	ch.cond = sync.NewCond(&ch.lock)
	return ch
}

func (c *RequestChan) Close() {
	c.lock.Lock()
	if !c.closed {
		c.closed = true
		c.cond.Broadcast()
	}
	c.lock.Unlock()
}

func (c *RequestChan) Buffered() int {
	c.lock.Lock()
	n := len(c.data)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PushBack(r *Request) int {
	c.lock.Lock()
	n := c.lockedPushBack(r)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PopFront() (*Request, bool) {
	c.lock.Lock()
	r, ok := c.lockedPopFront()
	c.lock.Unlock()
	return r, ok
}

func (c *RequestChan) lockedPushBack(r *Request) int {
	if c.closed {
		panic("send on closed chan")
	}
	if c.waits != 0 {
		c.cond.Signal()
	}
	c.data = append(c.data, r)
	return len(c.data)
}

func (c *RequestChan) lockedPopFront() (*Request, bool) {
	for len(c.data) == 0 {
		if c.closed {
			return nil, false
		}
		c.data = c.buff[:0]
		c.waits++
		c.cond.Wait()
		c.waits--
	}
	var r = c.data[0]
	c.data, c.data[0] = c.data[1:], nil
	return r, true
}

func (c *RequestChan) IsEmpty() bool {
	return c.Buffered() == 0
}

func (c *RequestChan) PopFrontAll(onRequest func(r *Request) error) error {
	for {
		r, ok := c.PopFront()
		if ok {
			if err := onRequest(r); err != nil {
				return err
			}
		} else {
			return nil
		}
	}
}

func (c *RequestChan) PopFrontAllVoid(onRequest func(r *Request)) {
	c.PopFrontAll(func(r *Request) error {
		onRequest(r)
		return nil
	})
}
