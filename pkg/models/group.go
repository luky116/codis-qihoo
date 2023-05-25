// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

const MaxGroupId = 9999

type Group struct {
	Id      int            `json:"id"`
	Servers []*GroupServer `json:"servers"`

	Promoting struct {
		Index int `json:"index,omitempty"`
		// 如果 server 发生了手动 【主->从，或是】 从->主 的动作，State会变为pendding
		// todo 应该是，只有 从->主 的时候，会改变这个字段的状态
		State string `json:"state,omitempty"`
	} `json:"promoting"`

	OutOfSync bool `json:"out_of_sync"`
}

type GroupServer struct {
	Addr       string `json:"server"`
	DataCenter string `json:"datacenter"`

	Action struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"action"`

	// 如果上次心跳失败，则置为true，不再提供服务，等待人工介入
	// todo 如果为true，不对外提供服务
	EverCrashed  bool `json:"ever_crashed"`
	ReplicaGroup bool `json:"replica_group"`
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}
