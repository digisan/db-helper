package badgerhelper

import (
	"bytes"

	"github.com/dgraph-io/badger/v3"
)

type DbAccessible interface {
	BadgerDB() *badger.DB
	Key() []byte
	Marshal(at any) (forKey []byte, forValue []byte)
	Unmarshal(dbKey []byte, dbVal []byte) (any, error)
}

type PtrDbAccessible[T any] interface {
	DbAccessible
	*T
}

// one object with fixed key
func GetOneObjectDB[V any, T PtrDbAccessible[V]](key []byte) (T, error) {
	var (
		found = false
		rt    = T(new(V))
		err   = rt.BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemproc := func(item *badger.Item) error {
				if bytes.Equal(key, item.Key()) {
					if err := item.Value(func(val []byte) error {
						_, err := rt.Unmarshal(key, val)
						found = true
						return err
					}); err != nil {
						return err
					}
				}
				return nil
			}

			if it.Seek(key); it.Valid() {
				return itemproc(it.Item())
			}

			return nil
		})
	)
	if !found {
		return nil, err
	}
	return rt, err
}

// use Unmarshal returned data as map-value
func GetMapDB[V any, T PtrDbAccessible[V]](prefix []byte) (map[string]any, error) {
	var (
		rt  = make(map[string]any)
		err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemproc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					key := item.Key()
					data, err := T(new(V)).Unmarshal(key, val)
					if err != nil {
						return err
					}
					rt[string(key)] = data
					return nil
				})
			}

			if len(prefix) == 0 {
				for it.Rewind(); it.Valid(); it.Next() {
					if err := itemproc(it.Item()); err != nil {
						return err
					}
				}
			} else {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if err := itemproc(it.Item()); err != nil {
						return err
					}
				}
			}

			return nil
		})
	)
	return rt, err
}

// all objects if prefix is nil or empty
func GetObjectsDB[V any, T PtrDbAccessible[V]](prefix []byte) ([]T, error) {
	var (
		rt  = []T{}
		err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemproc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					one := T(new(V))
					if _, err := one.Unmarshal(item.Key(), val); err != nil {
						return err
					}
					rt = append(rt, one)
					return nil
				})
			}

			if len(prefix) == 0 {
				for it.Rewind(); it.Valid(); it.Next() {
					if err := itemproc(it.Item()); err != nil {
						return err
					}
				}
			} else {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if err := itemproc(it.Item()); err != nil {
						return err
					}
				}
			}

			return nil
		})
	)
	return rt, err
}

// update or insert one object
func UpsertOneObjectDB[V any, T PtrDbAccessible[V]](object T) error {
	return object.BadgerDB().Update(func(txn *badger.Txn) error {
		return txn.Set(object.Marshal(nil))
	})
}

// update or insert part object at specific area
func UpsertPartObjectDB[V any, T PtrDbAccessible[V]](object T, at any) error {
	return object.BadgerDB().Update(func(txn *badger.Txn) error {
		return txn.Set(object.Marshal(at))
	})
}

// update or insert many objects
func UpsertObjectsDB[V any, T PtrDbAccessible[V]](objects ...T) error {
	wb := T(new(V)).BadgerDB().NewWriteBatch()
	defer wb.Cancel()

	for _, object := range objects {
		if err := wb.Set(object.Marshal(nil)); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// delete one object
func DeleteOneObjectDB[V any, T PtrDbAccessible[V]](key []byte) error {
	return T(new(V)).BadgerDB().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek(key); it.Valid() {
			if item := it.Item(); bytes.Equal(key, item.Key()) {
				return txn.Delete(item.KeyCopy(nil))
			}
		}
		return nil
	})
}
