// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"

	"github.com/CodisLabs/codis/pkg/models"
)

type Slot struct {
	id   int
	lock struct {
		hold bool
		sync.RWMutex
	}
	// 标记该slot上有未处理的请求
	// 确保更新slot信息的时候，该slot上的请求都处理完成
	refs sync.WaitGroup

	switched bool

	backend, migrate struct { // backend主要是处理redis读写操作的后端连接，
		id int // migrate是用来进行key迁移的连接。codis对redis进行了部分修改，支持了一些key迁移命令
		bc *sharedBackendConn
	}
	replicaGroups [][]*sharedBackendConn // 则是对应从节点的连接，如果设置允许读取从的话，读取命令会被发送的从节点。

	method forwardMethod
}

func (s *Slot) snapshot() *models.Slot {
	var m = &models.Slot{
		Id:     s.id,
		Locked: s.lock.hold,

		BackendAddr:        s.backend.bc.Addr(),
		BackendAddrGroupId: s.backend.id,
		MigrateFrom:        s.migrate.bc.Addr(),
		MigrateFromGroupId: s.migrate.id,
		ForwardMethod:      s.method.GetId(),
	}
	for i := range s.replicaGroups {
		var group []string
		for _, bc := range s.replicaGroups[i] {
			group = append(group, bc.Addr())
		}
		m.ReplicaGroups = append(m.ReplicaGroups, group)
	}
	return m
}

func (s *Slot) blockAndWait() {
	if !s.lock.hold {
		s.lock.hold = true
		s.lock.Lock()
	}
	s.refs.Wait()
}

func (s *Slot) unblock() {
	if !s.lock.hold {
		return
	}
	s.lock.hold = false
	s.lock.Unlock()
}

func (s *Slot) forward(r *Request, hkey []byte) error {
	return s.method.Forward(s, r, hkey)
}
