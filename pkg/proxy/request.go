// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Request struct {
	Multi []*redis.Resp   //保存请求命令,按redis的resp协议类型将请求保存到Multi字段中。比如，["GET","name"]
	Batch *sync.WaitGroup /*返回响应时,会在Batch处等待,r.Batch.Wait(), 所以可以做到当请求执行完成后才会执行返回函数*/
	Group *sync.WaitGroup

	Broken *atomic2.Bool

	OpStr string //输入指令，比如 GET、SET
	/**
	const (
		FlagWrite = 1 << iota
		FlagMasterOnly
		FlagMayWrite
		FlagNotAllow
	)
	*/
	OpFlag

	Database int32 //数据库编号
	UnixNano int64 //精确的unix时间

	*redis.Resp //保存响应数据,也是redis的resp协议类型
	Err         error

	Coalesce func() error //聚合函数,适用于mget/mset等需要聚合响应的操作命令
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

const GOLDEN_RATIO_PRIME_32 = 0x9e370001

func (r *Request) Seed16() uint {
	h32 := uint32(r.UnixNano) + uint32(uintptr(unsafe.Pointer(r)))
	h32 *= GOLDEN_RATIO_PRIME_32
	return uint(h32 >> 16)
}

type RequestChan struct {
	lock sync.Mutex
	cond *sync.Cond

	data []*Request // 如果有response，往这里放
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
	c.data = append(c.data, r) // 这里会将用户请求的处理结果放在data队列中。在 loopWriter 中，会将结果取出来，给用户相应
	return len(c.data)
}

func (c *RequestChan) lockedPopFront() (*Request, bool) {
	for len(c.data) == 0 { // data存放的是用户请求的处理结果，这里循环等待，直到有结果，再将结果返回给用户
		if c.closed {
			return nil, false
		}
		c.data = c.buff[:0]
		c.waits++
		c.cond.Wait() // 这里进入休眠，直到被 loopReader 唤醒
		c.waits--
	}
	var r = c.data[0]
	c.data, c.data[0] = c.data[1:], nil // 这里将 data[0]元素删除，以便继续处理data[1:]之后的元素。此操作就是堆的POP操作
	return r, true
}

func (c *RequestChan) IsEmpty() bool {
	return c.Buffered() == 0
}

func (c *RequestChan) PopFrontAll(onRequest func(r *Request) error) error { // 如果有请求进来，这里是请求的入口
	for {
		r, ok := c.PopFront() // 从 data 中读取执行结果
		if ok {
			if err := onRequest(r); err != nil { // onRequest 会将结果返回给用户
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
