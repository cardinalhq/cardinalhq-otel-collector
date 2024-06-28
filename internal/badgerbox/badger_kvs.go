package badgerbox

import (
	"errors"
	"time"

	"github.com/dgraph-io/badger"
)

type BadgerKVS struct {
	db *badger.DB
}

func NewBadgerKVS(db *badger.DB) *BadgerKVS {
	return &BadgerKVS{db: db}
}

var (
	_ KVS   = (*BadgerKVS)(nil)
	_ Wiper = (*BadgerKVS)(nil)
)

func (b *BadgerKVS) Get(key []byte) ([]byte, error) {
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (b *BadgerKVS) Set(key []byte, value []byte, ttl time.Duration) error {
	return b.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, value).WithTTL(ttl)
		return txn.SetEntry(entry)
	})
}

func (b *BadgerKVS) Delete(key []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *BadgerKVS) ForEachPrefix(prefix []byte, f func(key []byte, value []byte) bool) error {
	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if !f(k, v) {
				break
			}
		}
		return nil
	})
}

func (b *BadgerKVS) Maintain() error {
	return b.db.RunValueLogGC(0.5)
}

func (b *BadgerKVS) Close() error {
	return b.db.Close()
}

func (b *BadgerKVS) Wipe() error {
	return b.db.DropAll()
}
