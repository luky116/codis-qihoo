package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"sort"
)

func (s *Topom) CreateTable(name string, num int)  error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return  err
	}
	if err := models.ValidateTable(name); err != nil {
		return  err
	}

	var ids []int
	for id,table := range ctx.table  {
		if name == table.Name {
			return  errors.Errorf("name-[%s] already exists", name)
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var tid = 0
	for _, id := range ids {
		if id != tid  {
			break
		}
		tid ++
	}

	if tid > models.MaxTableNum {
		return  errors.Errorf("invalid table id = %d, out of range", tid)
	}
	defer s.dirtyTableCache(tid)

	t := &models.Table{
		Id:			tid,
		Name:		name,
		MaxSlotMum: num,
	}
	return  s.storeCreateTable(t)
}

func (s *Topom) ListTable() ([]*models.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	tSlice := make([]*models.Table, len(ctx.table))
	for _, t := range ctx.table {
		tSlice = append(tSlice, t)
	}
	return tSlice, nil
}

func (s *Topom) GetTable(tid int) (*models.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	if t, ok := ctx.table[tid]; ok {
		return t, nil
	} else {
		return nil, errors.Errorf("invalid table id = %d, not exist", tid)
	}
}

func (s *Topom) RemoveTable(tid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return  err
	}
	t := ctx.table[tid]
	if t == nil {
		return  errors.Errorf("table-[%d] not exist", tid)
	}
	for i, s := range ctx.slots[tid] {
		if s.GroupId != 0 {
			return  errors.Errorf("table-[%d] slot-[%d] is still in use, please off-line slot first", tid, i)
		}
	}
	for _, m := range ctx.slots[tid] {
			if err := s.storeRemoveSlotMapping(tid, m); err !=nil {
				return err
			}
		}
	defer s.dirtyTableCache(tid)
	return s.storeRemoveTable(t)
}





