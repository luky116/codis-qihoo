// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"sort"
	"time"

	rbtree "github.com/emirpasic/gods/trees/redblacktree"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
)

func (s *Topom) SlotCreateAction(sid int, gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", gid)
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if m.Action.State != models.ActionNothing {
		return errors.Errorf("slot-[%d] action already exists", sid)
	}
	if m.GroupId == gid {
		return errors.Errorf("slot-[%d] already in group-[%d]", sid, gid)
	}
	defer s.dirtySlotsCache(m.Id)

	m.Action.State = models.ActionPending
	m.Action.Index = ctx.maxSlotActionIndex() + 1
	m.Action.TargetId = g.Id
	return s.storeUpdateSlotMapping(m)
}

func (s *Topom) SlotCreateActionSome(groupFrom, groupTo int, numSlots int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(groupTo)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for _, m := range ctx.slots {
		if len(pending) >= numSlots {
			break
		}
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != groupFrom {
			continue
		}
		if m.GroupId == g.Id {
			continue
		}
		pending = append(pending, m.Id)
	}

	for _, sid := range pending {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = g.Id
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

// slot 迁移逻辑
func (s *Topom) SlotCreateActionRange(beg, end int, gid int, must bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if !(beg >= 0 && beg <= end && end < MaxSlotNum) {
		return errors.Errorf("invalid slot range [%d,%d]", beg, end)
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 { // 如果组没有配置server id，说明这个group不生效，不能往非法的group迁移
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for sid := beg; sid <= end; sid++ {
		// 获取slot
		slot, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		// 如果 slot 的状态异常
		if slot.Action.State != models.ActionNothing {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] action already exists", sid)
		}
		// slot 已经在 group 了，所以直接返回
		if slot.GroupId == g.Id {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] already in group-[%d]", sid, g.Id)
		}
		pending = append(pending, slot.Id)
	}

	for _, sid := range pending {
		slot, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(slot.Id)

		slot.Action.State = models.ActionPending
		// 当前正在操作的最大index
		slot.Action.Index = ctx.maxSlotActionIndex() + 1
		slot.Action.TargetId = g.Id
		// 更新元数据配置中该slot的状态为pending
		if err := s.storeUpdateSlotMapping(slot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SlotRemoveAction(sid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if m.Action.State == models.ActionNothing {
		return errors.Errorf("slot-[%d] action doesn't exist", sid)
	}
	if m.Action.State != models.ActionPending {
		return errors.Errorf("slot-[%d] action isn't pending", sid)
	}
	defer s.dirtySlotsCache(m.Id)

	m = &models.SlotMapping{
		Id:      m.Id,
		GroupId: m.GroupId,
	}
	return s.storeUpdateSlotMapping(m)
}

func (s *Topom) SlotActionPrepare() (int, bool, error) {
	return s.SlotActionPrepareFilter(nil, nil)
}

func (s *Topom) SlotActionPrepareFilter(accept, update func(m *models.SlotMapping) bool) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return 0, false, err
	}

	var minActionIndex = func(filter func(m *models.SlotMapping) bool) (picked *models.SlotMapping) {
		for _, m := range ctx.slots {
			if m.Action.State == models.ActionNothing {
				continue
			}
			if filter(m) {
				//if filter(m) {
				//从最小的开始执行
				if picked != nil && picked.Action.Index < m.Action.Index {
					continue
				}
				//只有一个slot没有执行过update方法，accept才会返回true；也就是说，即一个slot只会被处理一次
				if accept == nil || accept(m) {
					picked = m
				}
			}
		}
		return picked
	}

	var m = func() *models.SlotMapping {
		// 取出 m.Action.State != "" and m.Action.State != pending 的slot里面action.id最小的那个slot
		// 即: 取出正在做slot迁移的slot里面action.id最小的那个slot
		// 赋值给picked
		// 然后返回
		var picked = minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State != models.ActionPending
		})
		if picked != nil {
			return picked
		}
		if s.action.disabled.IsTrue() {
			return nil
		}
		// 取出 m.Action.State != "" and m.Action.State == models.ActionPending的slot里面action.id最小的那个slot
		// 即: 取出pending状态的slot里面action.id最小的那个slot
		// 赋值给picked
		// 然后返回
		return minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State == models.ActionPending
		})
	}()

	if m == nil {
		return 0, false, nil
	}

	if update != nil && !update(m) {
		return 0, false, nil
	}

	log.Warnf("slot-[%d] action prepare:\n%s", m.Id, m.Encode())

	//变更每个SlotMapping的action.state,并与zk交互
	//另外,Action.state符合preparing或者prepared的时候,要将SlotMapping同步到proxy
	switch m.Action.State {

	case models.ActionPending:

		defer s.dirtySlotsCache(m.Id)

		// 改状态为准备中迁移
		m.Action.State = models.ActionPreparing
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}

		fallthrough

	case models.ActionPreparing:

		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] resync to prepared", m.Id)
		// 改状态为已准备迁移
		m.Action.State = models.ActionPrepared
		// 通过Proxy重连新的Redis Server并且设置slot从哪迁移等信息
		// 将slotMapping信息刷新到proxy中, 如果刷失败了, 将m.Action.State改回ActionPreparing, 返回
		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync-rollback to preparing", m.Id)
			m.Action.State = models.ActionPreparing
			s.resyncSlotMappings(ctx, m)
			log.Warnf("slot-[%d] resync-rollback to preparing, done", m.Id)
			return 0, false, err
		}
		// 刷新proxy信息成功后, 将m.Action.State = models.ActionPrepared写入到zk中
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}

		fallthrough

	case models.ActionPrepared:

		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] resync to migrating", m.Id)

		// 将action state状态改成 ActionMigrating
		m.Action.State = models.ActionMigrating
		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync to migrating failed", m.Id)
			return 0, false, err
		}
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}

		fallthrough

	case models.ActionMigrating:

		return m.Id, true, nil

	case models.ActionFinished:

		return m.Id, true, nil

	default:

		return 0, false, errors.Errorf("slot-[%d] action state is invalid", m.Id)

	}
}

func (s *Topom) SlotActionComplete(sid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}

	log.Warnf("slot-[%d] action complete:\n%s", m.Id, m.Encode())

	switch m.Action.State {

	case models.ActionMigrating:

		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionFinished
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}

		fallthrough

	case models.ActionFinished:

		log.Warnf("slot-[%d] resync to finished", m.Id)

		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync to finished failed", m.Id)
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m = &models.SlotMapping{
			Id:      m.Id,
			GroupId: m.Action.TargetId,
		}
		return s.storeUpdateSlotMapping(m)

	default:

		return errors.Errorf("slot-[%d] action state is invalid", m.Id)

	}
}

// 得到执行器
func (s *Topom) newSlotActionExecutor(sid int) (func(db int) (remains int, nextdb int, err error), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return nil, err
	}

	switch m.Action.State {

	case models.ActionMigrating:

		if s.action.disabled.IsTrue() {
			return nil, nil
		}
		// m.groupId 主从在切换时, 不做slot迁移操作
		if ctx.isGroupPromoting(m.GroupId) {
			return nil, nil
		}
		// m.action.targetId 主从在切换时, 不做slot迁移操作
		if ctx.isGroupPromoting(m.Action.TargetId) {
			return nil, nil
		}

		from := ctx.getGroupMaster(m.GroupId)
		dest := ctx.getGroupMaster(m.Action.TargetId)

		s.action.executor.Incr()

		return func(db int) (int, int, error) {
			defer s.action.executor.Decr()
			if from == "" {
				return 0, -1, nil
			}
			//从cache中得到group 1的redisClient,这个client由addr, auth, timeout，Database，redigo.Conn组成: 如果cache没有, 就新建
			c, err := s.action.redisp.GetClient(from)
			if err != nil {
				return 0, -1, err
			}
			//将刚才新建的或者从cache中取出的redis client再put到Topom.action.redisp中
			defer s.action.redisp.PutClient(c)

			if err := c.Select(db); err != nil {
				return 0, -1, err
			}
			var do func() (int, error)

			method, _ := models.ParseForwardMethod(s.config.MigrationMethod)
			switch method {
			case models.ForwardSync:
				do = func() (int, error) {
					//调用redis的SLOTSMGRTTAGSLOT命令,随机选择当前slot的一个key,并将与这个key的tag相同的k-v全部迁移到目标机
					return c.MigrateSlot(sid, dest)
				}
			case models.ForwardSemiAsync:
				var option = &redis.MigrateSlotAsyncOption{
					MaxBulks: s.config.MigrationAsyncMaxBulks,
					MaxBytes: s.config.MigrationAsyncMaxBytes.AsInt(),
					NumKeys:  s.config.MigrationAsyncNumKeys,
					Timeout: math2.MinDuration(time.Second*5,
						s.config.MigrationTimeout.Duration()),
				}
				//调用redis的SLOTSMGRTTAGSLOT-ASYNC命令,参数是target redis的ip和port
				do = func() (int, error) {
					return c.MigrateSlotAsync(sid, dest, option)
				}
			default:
				log.Panicf("unknown forward method %d", int(method))
			}

			n, err := do()
			if err != nil {
				return 0, -1, err
			} else if n != 0 {
				return n, db, nil
			}

			nextdb := -1
			//通过info命令查keyspace信息并做处理,这里取出的m为空
			m, err := c.InfoKeySpace()
			if err != nil {
				return 0, -1, err
			}
			for i := range m {
				if (nextdb == -1 || i < nextdb) && db < i {
					nextdb = i
				}
			}
			return 0, nextdb, nil

		}, nil

	case models.ActionFinished:

		return func(int) (int, int, error) {
			return 0, -1, nil
		}, nil

	default:

		return nil, errors.Errorf("slot-[%d] action state is invalid", m.Id)

	}
}

func (s *Topom) SlotsAssignGroup(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		g, err := ctx.getGroup(m.GroupId)
		if err != nil {
			return err
		}
		if len(g.Servers) == 0 {
			return errors.Errorf("group-[%d] is empty", g.Id)
		}
		if m.Action.State != models.ActionNothing {
			return errors.Errorf("invalid slot-[%d] action = %s", m.Id, m.Action.State)
		}
	}

	for i, m := range slots {
		if g := ctx.group[m.GroupId]; !g.OutOfSync {
			defer s.dirtyGroupCache(g.Id)
			g.OutOfSync = true
			if err := s.storeUpdateGroup(g); err != nil {
				return err
			}
		}
		slots[i] = &models.SlotMapping{
			Id: m.Id, GroupId: m.GroupId,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d]", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

func (s *Topom) SlotsAssignOffline(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		if m.GroupId != 0 {
			return errors.Errorf("group of slot-[%d] should be 0", m.Id)
		}
	}

	for i, m := range slots {
		slots[i] = &models.SlotMapping{
			Id: m.Id,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d] (offline)", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

func (s *Topom) SlotsRebalance(confirm bool) (map[int]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	var groupIds []int
	for _, g := range ctx.group {
		if len(g.Servers) != 0 {
			groupIds = append(groupIds, g.Id)
		}
	}
	sort.Ints(groupIds)

	if len(groupIds) == 0 {
		return nil, errors.Errorf("no valid group could be found")
	}

	var (
		assigned = make(map[int]int)
		pendings = make(map[int][]int)
		moveout  = make(map[int]int)
		docking  []int
	)
	var groupSize = func(gid int) int {
		return assigned[gid] + len(pendings[gid]) - moveout[gid]
	}

	// don't migrate slot if it's being migrated
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			assigned[m.Action.TargetId]++
		}
	}

	var lowerBound = MaxSlotNum / len(groupIds)

	// don't migrate slot if groupSize < lowerBound
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != 0 {
			if groupSize(m.GroupId) < lowerBound {
				assigned[m.GroupId]++
			} else {
				pendings[m.GroupId] = append(pendings[m.GroupId], m.Id)
			}
		}
	}

	var tree = rbtree.NewWith(func(x, y interface{}) int {
		var gid1 = x.(int)
		var gid2 = y.(int)
		if gid1 != gid2 {
			if d := groupSize(gid1) - groupSize(gid2); d != 0 {
				return d
			}
			return gid1 - gid2
		}
		return 0
	})
	for _, gid := range groupIds {
		tree.Put(gid, nil)
	}

	// assign offline slots to the smallest group
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != 0 {
			continue
		}
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		docking = append(docking, m.Id)
		moveout[dest]--

		tree.Put(dest, nil)
	}

	var upperBound = (MaxSlotNum + len(groupIds) - 1) / len(groupIds)

	// rebalance between different server groups
	for tree.Size() >= 2 {
		from := tree.Right().Key.(int)
		tree.Remove(from)

		if len(pendings[from]) == moveout[from] {
			continue
		}
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		var (
			fromSize = groupSize(from)
			destSize = groupSize(dest)
		)
		if fromSize <= lowerBound {
			break
		}
		if destSize >= upperBound {
			break
		}
		if d := fromSize - destSize; d <= 1 {
			break
		}
		moveout[from]++
		moveout[dest]--

		tree.Put(from, nil)
		tree.Put(dest, nil)
	}

	for gid, n := range moveout {
		if n < 0 {
			continue
		}
		if n > 0 {
			sids := pendings[gid]
			sort.Sort(sort.Reverse(sort.IntSlice(sids)))

			docking = append(docking, sids[0:n]...)
			pendings[gid] = sids[n:]
		}
		delete(moveout, gid)
	}
	sort.Ints(docking)

	var plans = make(map[int]int)

	for _, gid := range groupIds {
		var in = -moveout[gid]
		for i := 0; i < in && len(docking) != 0; i++ {
			plans[docking[0]] = gid
			docking = docking[1:]
		}
	}

	if !confirm {
		return plans, nil
	}

	var slotIds []int
	for sid, _ := range plans {
		slotIds = append(slotIds, sid)
	}
	sort.Ints(slotIds)

	for _, sid := range slotIds {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return nil, err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = plans[sid]
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return nil, err
		}
	}
	return plans, nil
}
