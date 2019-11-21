package generic

import (
	"github.com/nsf/sqlbatch/util"
	"strings"
)

// TODO: add more generic types, explain when and how to use them

type Int64 int64

func (v Int64) SqlbatchConv(b *strings.Builder)       { util.AppendInt64(b, int64(v), false) }
func (v *Int64) SqlbatchGet(ifacePtr *interface{})    { *ifacePtr = (int64)(*v) }
func (v *Int64) SqlbatchGetPtr(ifacePtr *interface{}) { *ifacePtr = (*int64)(v) }
func (v *Int64) SqlbatchSet(iface interface{})        { *v = Int64(iface.(int64)) }
func (v *Int64) SqlbatchWrite(b *strings.Builder)     { util.AppendInt64(b, int64(*v), false) }
