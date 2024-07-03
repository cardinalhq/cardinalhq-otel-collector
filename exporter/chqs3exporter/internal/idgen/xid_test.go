package idgen

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestXidGenerator_Make(t *testing.T) {
	x := NewXIDGenerator()
	id := x.Make(time.Now())
	assert.NotEmpty(t, id, "failed to generate ID")
	assert.Len(t, id, 20, "ID has wrong length")
}

func TestXidGenerator_Make_many(t *testing.T) {
	x := NewXIDGenerator()
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := x.Make(time.Now())
		ids[id] = true
	}
	assert.Len(t, ids, 1000, "failed to generate unique IDs")
	for id := range ids {
		assert.Len(t, id, 20, "ID %s has wrong length", id)
	}
}
