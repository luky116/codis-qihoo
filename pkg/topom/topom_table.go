package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func (s *Topom) CreateTable(name string, num ,tid int)  error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return  err
	}
	if err := models.ValidateTable(name); err != nil {
		return  err
	}
	if tid != -1 {
		if _, ok := ctx.table[tid]; ok == true {
			return  errors.Errorf("tid-[%d] already exists", tid)
		}
		if tid >= ctx.tableMeta.Id {
			return  errors.Errorf("tid-[%d] is lagre than self-increase Id-[%d],please change tid and retry", tid, ctx.tableMeta.Id)
		}
	} else {
		tid = ctx.tableMeta.Id
		defer s.dirtyTableMetaCache()
		tm := &models.TableMeta{Id: tid+1}
		if err := s.storeCreateTableMeta(tm); err != nil {
			return err
		}
	}
	for _, t := range ctx.table  {
		if name == t.Name {
			return  errors.Errorf("name-[%s] already exists", name)
		}
		if tid == t.Id {
			return  errors.Errorf("tid-[%d] already exists", tid)
		}
	}
	defer s.dirtyTableCache(tid)
	t := &models.Table{
		Id:			tid,
		Name:		name,
		MaxSlotMum: num,
	}
	if err := s.storeCreateTable(t); err != nil {
		return err
	}
	if err := s.syncCreateTable(ctx, t); err != nil {
		log.Warnf("table-[%s] tid-[%d] sync to proxy failed", t.Name, t.Id)
		return  err
	}
	return nil
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
	if err := s.syncRemoveTable(ctx, t); err != nil {
		log.Warnf("table-[%s] tid-[%d] sync to proxy failed", t.Name, t.Id)
		return err
	}
	defer s.dirtyTableCache(tid)
	return s.storeRemoveTable(t)
}

func (s *Topom) RenameTable(tid int, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return  err
	}
	if err := models.ValidateTable(name); err != nil {
		return  err
	}
	for _, t := range ctx.table  {
		if name == t.Name {
			return  errors.Errorf("name-[%s] already exists", name)
		}
	}
	t, ok := ctx.table[tid]
	if ok == false {
		return  errors.Errorf("table-[%d] dose not exist", tid)
	}
	defer s.dirtyTableCache(tid)
	t.Name = name
	return s.storeUpdateTable(t)
}

func (s *Topom) GetTableMeta() (int, error){
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return  0, err
	}
	return ctx.tableMeta.Id, nil
}

func (s *Topom) SetTableMeta(id int) error{
	defer s.dirtyTableMetaCache()
	tm := &models.TableMeta{Id: id}
	return s.storeCreateTableMeta(tm)
}



