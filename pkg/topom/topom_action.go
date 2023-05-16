// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"strconv"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

// 后台服务，定时扫描slot，后台继续操作
func (s *Topom) ProcessSlotAction() error {
	for s.IsOnline() {
		var (
			marks = make(map[int]bool)
			plans = make(map[int]bool)
		)
		var accept = func(m *models.SlotMapping) bool {
			// 一个group，同时只有一个slot迁移出去或一个slot迁移进来
			if marks[m.GroupId] || marks[m.Action.TargetId] {
				return false
			}
			// slot即将处理，无需再次添加
			if plans[m.Id] { //slot维度迁移，所以不能并行处理
				return false
			}
			return true
		}
		// 标记group或是目标group在处理中
		var update = func(m *models.SlotMapping) bool {
			if m.GroupId != 0 {
				marks[m.GroupId] = true
			}
			marks[m.Action.TargetId] = true
			plans[m.Id] = true
			return true
		}
		// 并行迁移协程数量，用户通过配置文件指定
		var parallel = math2.MaxInt(1, s.config.MigrationParallelSlots)
		// 往plans里面加入parallel个待处理的数据，以便后面并行处理
		for parallel > len(plans) {
			//状态转移在这里完成
			_, ok, err := s.SlotActionPrepareFilter(accept, update)
			if err != nil {
				return err
			} else if !ok {
				break
			}
		}
		if len(plans) == 0 {
			return nil
		}
		var fut sync2.Future
		for sid, _ := range plans {
			fut.Add()
			go func(sid int) {
				log.Warnf("slot-[%d] process action", sid)
				//重点，真正的数据迁移
				var err = s.processSlotAction(sid)
				if err != nil {
					status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
					s.action.progress.status.Store(status)
				} else {
					s.action.progress.status.Store("")
				}
				fut.Done(strconv.Itoa(sid), err)
			}(sid)
		}
		for _, v := range fut.Wait() {
			if v != nil {
				return v.(error)
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	return nil
}

// 迁移逻辑
func (s *Topom) processSlotAction(sid int) error {
	var db int = 0
	// 不停重试，直到OK
	for s.IsOnline() {
		if exec, err := s.newSlotActionExecutor(sid); err != nil {
			return err
		} else if exec == nil {
			time.Sleep(time.Second)
		} else {
			// 这里使用了slotsmgrttagslot方法迁移，每次只会迁移一个key。所以迁移的效率并不高
			n, nextdb, err := exec(db)
			if err != nil {
				return err
			}
			log.Debugf("slot-[%d] action executor %d", sid, n)
			//迁移完成判断
			if n == 0 && nextdb == -1 {
				return s.SlotActionComplete(sid)
			}
			status := fmt.Sprintf("[OK] Slot[%04d]@DB[%d]=%d", sid, db, n)
			s.action.progress.status.Store(status)

			if us := s.GetSlotActionInterval(); us != 0 {
				time.Sleep(time.Microsecond * time.Duration(us))
			}
			db = nextdb
		}
	}
	return nil
}

func (s *Topom) ProcessSyncAction() error {
	addr, err := s.SyncActionPrepare()
	if err != nil || addr == "" {
		return err
	}
	log.Warnf("sync-[%s] process action", addr)

	exec, err := s.newSyncActionExecutor(addr)
	if err != nil || exec == nil {
		return err
	}
	return s.SyncActionComplete(addr, exec() != nil)
}
