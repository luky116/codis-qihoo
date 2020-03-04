// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.
package models

const MaxTableNum  int = 9999

type Table struct {
	Id		int			`json:"id"`
	Name	string		`json:"name"`
	MaxSlotMum int		`json:"max_slot_mum"`
//	Slots   []*Slot 	`json:"slots"`
//	Group  []*Group		`json:"groups"`
}

func (t *Table) Encode() []byte {
	return jsonEncode(t)
}

