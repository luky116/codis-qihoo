package topom

import (
	"github.com/CodisLabs/codis/pkg/utils/assert"
	"testing"
)

func TestCreateTable(x *testing.T) {
	t := openTopom()
	defer t.Close()

	var name = "table1"
	const num = 128

	assert.MustNoError(t.CreateTable(name,num))

}

func createTable(t *Topom) {
	var name = "table1"
	const num = 128
	assert.MustNoError(t.CreateTable(name,num))
}