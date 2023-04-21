// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

/**
文件路径：//路径是/codis3/codis-demo/topom
数据格式如下：
{
    "token": "af6f1d222029e2f6499457951167d3bc",
    "start_time": "2023-04-20 18:08:13.703135 +0800 CST m=+0.035513443",
    "admin_addr": "sanyuedeMacBook-Pro.local:18080",
    "product_name": "codis-demo",
    "pid": 1042,
    "pwd": "/Users/sanyue/go/src/github.com/CodisLabs/codis",
    "sys": "Darwin sanyuedeMacBook-Pro.local 22.3.0 Darwin Kernel Version 22.3.0: Mon Jan 30 20:42:11 PST 2023; root:xnu-8792.81.3~2/RELEASE_X86_64 x86_64"
}
*/
type Topom struct {
	Token     string `json:"token"`
	StartTime string `json:"start_time"`
	AdminAddr string `json:"admin_addr"`

	ProductName string `json:"product_name"`

	Pid int    `json:"pid"`
	Pwd string `json:"pwd"`
	Sys string `json:"sys"`
}

func (t *Topom) Encode() []byte {
	return jsonEncode(t)
}
