package idgen

import (
	"time"

	"github.com/rs/xid"
)

type XidGenerator struct{}

var _ IDGenerator = (*XidGenerator)(nil)

func NewXIDGenerator() *XidGenerator {
	return &XidGenerator{}
}

func (x *XidGenerator) Make(_ time.Time) string {
	return xid.New().String()
}
