package badgerhelper

import (
	"bytes"
	"errors"

	"github.com/dgraph-io/badger/v4"
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
func GetOneObject[V any, T PtrDbAccessible[V]](key []byte) (T, error) {
	var (
		found = false
		rt    = T(new(V))
		err   = rt.BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemProc := func(item *badger.Item) error {
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
				return itemProc(it.Item())
			}
			return nil
		})
	)
	if !found {
		return nil, err
	}
	return rt, err
}

// use Unmarshal returned data as map-value, filter key is []byte type
func GetMap[V any, T PtrDbAccessible[V]](prefix []byte, filter func([]byte, any) bool) (map[string]any, error) {
	var (
		rt  = make(map[string]any)
		err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemProc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					key := item.Key()
					data, err := T(new(V)).Unmarshal(key, val)
					if err != nil {
						return err
					}
					if filter == nil || filter(key, data) {
						rt[string(key)] = data
					}
					return nil
				})
			}
			if len(prefix) == 0 {
				for it.Rewind(); it.Valid(); it.Next() {
					if err := itemProc(it.Item()); err != nil {
						return err
					}
				}
			} else {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if err := itemProc(it.Item()); err != nil {
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
func GetObjects[V any, T PtrDbAccessible[V]](prefix []byte, filter func(T) bool) ([]T, error) {
	var (
		rt  = []T{}
		err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemProc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					one := T(new(V))
					if _, err := one.Unmarshal(item.Key(), val); err != nil {
						return err
					}
					if filter == nil || filter(one) {
						rt = append(rt, one)
					}
					return nil
				})
			}
			if len(prefix) == 0 {
				for it.Rewind(); it.Valid(); it.Next() {
					if err := itemProc(it.Item()); err != nil {
						return err
					}
				}
			} else {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if err := itemProc(it.Item()); err != nil {
						return err
					}
				}
			}
			return nil
		})
	)
	return rt, err
}

func GetObjectCount[V any, T PtrDbAccessible[V]](prefix []byte, filter func(T) bool) (int, error) {
	var (
		n   = 0
		err = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemProc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					one := T(new(V))
					if _, err := one.Unmarshal(item.Key(), val); err != nil {
						n = 0
						return err
					}
					if filter == nil || filter(one) {
						n++
					}
					return nil
				})
			}
			if len(prefix) == 0 {
				for it.Rewind(); it.Valid(); it.Next() {
					if err := itemProc(it.Item()); err != nil {
						return err
					}
				}
			} else {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if err := itemProc(it.Item()); err != nil {
						return err
					}
				}
			}
			return nil
		})
	)
	return n, err
}

func GetFirstObject[V any, T PtrDbAccessible[V]](prefix []byte, filter func(T) bool) (T, error) {
	var (
		found = false
		rt    = T(new(V))
		err   = T(new(V)).BadgerDB().View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			itemProc := func(item *badger.Item) error {
				return item.Value(func(val []byte) error {
					one := T(new(V))
					if _, err := one.Unmarshal(item.Key(), val); err != nil {
						return err
					}
					if filter == nil || filter(one) {
						found = true
						rt = one
					}
					return nil
				})
			}
			if len(prefix) == 0 {
				for it.Rewind(); it.Valid(); it.Next() {
					if err := itemProc(it.Item()); err != nil {
						return err
					}
					if found {
						break
					}
				}
			} else {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if err := itemProc(it.Item()); err != nil {
						return err
					}
					if found {
						break
					}
				}
			}
			return nil
		})
	)
	if !found {
		return nil, err
	}
	return rt, err
}

// -------------------------------------------------------------------- //

// update or insert one object
func UpsertOneObject[V any, T PtrDbAccessible[V]](object T) error {
	return object.BadgerDB().Update(func(txn *badger.Txn) error {
		return txn.Set(object.Marshal(nil))
	})
}

// update or insert part object at specific area
func UpsertPartObject[V any, T PtrDbAccessible[V]](object T, at any) error {
	return object.BadgerDB().Update(func(txn *badger.Txn) error {
		return txn.Set(object.Marshal(at))
	})
}

// update or insert many objects
func UpsertObjects[V any, T PtrDbAccessible[V]](objects ...T) error {
	wb := T(new(V)).BadgerDB().NewWriteBatch()
	defer wb.Cancel()

	for _, object := range objects {
		if err := wb.Set(object.Marshal(nil)); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// -------------------------------------------------------------------- //

// delete one object
func DeleteOneObject[V any, T PtrDbAccessible[V]](key []byte) (n int, err error) {
	return n, T(new(V)).BadgerDB().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek(key); it.Valid() {
			if item := it.Item(); bytes.Equal(key, item.Key()) {
				if err = txn.Delete(item.KeyCopy(nil)); err == nil {
					n++
				}
			}
		}
		return err
	})
}

// delete multiple objects
func DeleteObjects[V any, T PtrDbAccessible[V]](prefix []byte) (n int, err error) {
	return n, T(new(V)).BadgerDB().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err = txn.Delete(it.Item().KeyCopy(nil)); err == nil {
				n++
			} else {
				break
			}
		}
		return err
	})
}

func DeleteFirstObject[V any, T PtrDbAccessible[V]](prefix []byte) (n int, err error) {
	return n, T(new(V)).BadgerDB().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek(prefix); it.ValidForPrefix(prefix) {
			if err = txn.Delete(it.Item().KeyCopy(nil)); err == nil {
				n++
			}
		}
		return err
	})
}

// -------------------------------------------------------------------- //

func UpdateFirstObject[V any, T PtrDbAccessible[V]](prefix []byte, object T) (n int, err error) {

	if len(object.Key()) == 0 {
		return 0, errors.New("object.Key CANNOT be empty")
	}
	if len(prefix) == 0 {
		return 0, errors.New("prefix CANNOT be empty")
	}
	if !bytes.HasPrefix(object.Key(), prefix) {
		return 0, errors.New("object.Key MUST start with input prefix")
	}

	return n, T(new(V)).BadgerDB().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Seek(prefix); it.ValidForPrefix(prefix) {
			if err = txn.Delete(it.Item().KeyCopy(nil)); err == nil {
				n++
				return UpsertOneObject(object)
			}
		}
		return err
	})
}
