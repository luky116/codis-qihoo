// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

func (s *Topom) CreateProxy(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed, %s", addr, err)
	}
	c := s.newProxyClient(p)

	// 确认 proxy 是否在运行
	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed, %s", addr, err)
	}
	if ctx.proxy[p.Token] != nil {
		return errors.Errorf("proxy-[%s] already exists", p.Token)
	} else {
		p.Id = ctx.maxProxyId() + 1
	}
	defer s.dirtyProxyCache(p.Token)

	// 添加缓存
	if err := s.storeCreateProxy(p); err != nil {
		return err
	} else {
		return s.reinitProxy(ctx, p, c)
	}
}

func (s *Topom) OnlineProxy(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	// 配置中心proxy路径下的文件
	/**
	{
	    "id": 1,
	    "token": "9cad0f626c77b14740922be2b0a152b7",
	    "start_time": "2023-04-25 23:02:24.857297 +0800 CST m=+0.019739280",
	    "admin_addr": "sanyuedeMacBook-Pro.local:11080",
	    "proto_type": "tcp4",
	    "proxy_addr": "sanyuedeMacBook-Pro.local:19000",
	    "product_name": "codis-demo",
	    "pid": 3899,
	    "pwd": "/Users/sanyue/go/src/github.com/CodisLabs/codis",
	    "sys": "Darwin sanyuedeMacBook-Pro.local 22.3.0 Darwin Kernel Version 22.3.0: Mon Jan 30 20:42:11 PST 2023; root:xnu-8792.81.3~2/RELEASE_X86_64 x86_64",
	    "hostname": "sanyuedeMacBook-Pro.local",
	    "datacenter": ""
	}
	*/
	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed", addr)
	}

	//这个ApiClient中存储了proxy的地址，以及根据productName,productAuth(默认为空)以及token生成的auth
	c := s.newProxyClient(p)

	// proxy启动的时候，在s.setup(config)这一步，会生成一个xauth，存储在Proxy的xauth属性中，
	// 这一步就是讲上面得到的xauth和启动proxy时的xauth作比较，来唯一确定需要的xauth
	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed", addr)
	}
	defer s.dirtyProxyCache(p.Token)

	//检查上下文中的proxy是否已经有token，如果有的话，说明这个proxy已经添加到集群了
	if d := ctx.proxy[p.Token]; d != nil {
		p.Id = d.Id
		if err := s.storeUpdateProxy(p); err != nil {
			return err
		}
	} else {
		//上下文中所有proxy的最大id+1，赋给当前的proxy作为其id
		p.Id = ctx.maxProxyId() + 1
		//到这一步，proxy已经添加成功，更新"/codis3/codis-wujiang/proxy/proxy-token"下面的proxy信息
		if err := s.storeCreateProxy(p); err != nil {
			return err
		}
	}
	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) RemoveProxy(token string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(p)

	if err := c.Shutdown(); err != nil {
		log.WarnErrorf(err, "proxy-[%s] shutdown failed, force remove = %t", token, force)
		if !force {
			return errors.Errorf("proxy-[%s] shutdown failed", p.Token)
		}
	}
	defer s.dirtyProxyCache(p.Token)

	return s.storeRemoveProxy(p)
}

func (s *Topom) ReinitProxy(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(p)

	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) newProxyClient(p *models.Proxy) *proxy.ApiClient {
	c := proxy.NewApiClient(p.AdminAddr)
	c.SetXAuth(s.config.ProductName, s.config.ProductAuth, p.Token)
	return c
}

func (s *Topom) reinitProxy(ctx *context, p *models.Proxy, c *proxy.ApiClient) error {
	log.Warnf("proxy-[%s] reinit:\n%s", p.Token, p.Encode())
	//初始化1024个槽
	if err := c.FillSlots(ctx.toSlotSlice(ctx.slots, p)...); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] fillslots failed", p.Token)
		return errors.Errorf("proxy-[%s] fillslots failed", p.Token)
	}
	if err := c.Start(); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] start failed", p.Token)
		return errors.Errorf("proxy-[%s] start failed", p.Token)
	}
	//由于此时sentinels还没有，传入的server队列为空，所以这个方法我们暂时可以不管
	if err := c.SetSentinels(ctx.sentinel); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] set sentinels failed", p.Token)
		return errors.Errorf("proxy-[%s] set sentinels failed", p.Token)
	}
	return nil
}

func (s *Topom) resyncSlotMappingsByGroupId(ctx *context, gid int) error {
	return s.resyncSlotMappings(ctx, ctx.getSlotMappingsByGroupId(gid)...)
}

func (s *Topom) resyncSlotMappings(ctx *context, slots ...*models.SlotMapping) error {
	if len(slots) == 0 {
		return nil
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(p).FillSlots(ctx.toSlotSlice(slots, p)...)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync slots failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] resync slots failed", t)
			}
		}
	}
	return nil
}
