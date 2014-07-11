package gopark

import (
	"testing"
)

func TestContext(t *testing.T) {
	ctx := NewContext("gopark")
	ctx.init()
	t.Logf("%#v\n", ctx)
	t.Log("ok")
}
