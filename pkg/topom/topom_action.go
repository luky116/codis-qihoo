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
			// 标记该 slot 已经被执行过迁移
			// marks里面保存的是:已经分配了group,或者即将分配group,这2种group id
			marks = make(map[int]bool)
			// 即将要做迁移放入slot id
			plans = make(map[int]bool)
		)
		// 如果执行了 update，那么 marks 里面就会有值，
		// 也就是说，一个slot只会被执行一次update，确保一个slot只会被迁移一次
		// 如果m的即将分配group id在marks里面, accept(m)就返回false, 这样就保证了同时只有一个slot迁入到同一个group下, 在一个redis下面,同时只有一个slot被迁移出去
		var accept = func(m *models.SlotMapping) bool {
			if marks[m.GroupId] || marks[m.Action.TargetId] {
				return false
			}
			if plans[m.Id] {
				return false
			}
			return true
		}
		//对plans和marks进行初始化
		var update = func(m *models.SlotMapping) bool {
			//只有在槽当前的GroupId为0的时候，marks[m.GroupId]才是false
			if m.GroupId != 0 {
				marks[m.GroupId] = true
			}
			marks[m.Action.TargetId] = true
			plans[m.Id] = true
			return true
		}
		//按照默认的配置文件，这个值是100，并行迁移的slot数量，是一个阀值
		var parallel = math2.MaxInt(1, s.config.MigrationParallelSlots) // 并行迁移协程数量
		// 往plans里面添加记录，直到plans的size等于parallel
		// 这样配合后面的 for plans，就能达到约定的并行度
		for parallel > len(plans) {
			// 更改zk中slot的状态，并通知proxy进行更改
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
				// 对上面修改状态的slot进行迁移
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
