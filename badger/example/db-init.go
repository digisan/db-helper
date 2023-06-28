package example

import (
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	lk "github.com/digisan/logkit"
)

var (
	onceEDB sync.Once // do once
	dbGrp   *DBGrp    // global, for keeping single instance
)

type DBGrp struct {
	sync.Mutex
	db1 *badger.DB
	db2 *badger.DB
}

func open(dir string) *badger.DB {
	opt := badger.DefaultOptions("").WithInMemory(true)
	if dir != "" {
		opt = badger.DefaultOptions(dir)
		opt.Logger = nil
	}
	db, err := badger.Open(opt)
	lk.FailOnErr("%v", err)
	return db
}

// init global 'dbGrp'
func InitDB(dir string) *DBGrp {
	if dbGrp == nil {
		onceEDB.Do(func() {
			dbGrp = &DBGrp{
				db1: open(filepath.Join(dir, "db1")),
				db2: open(filepath.Join(dir, "db2")),
			}
		})
	}
	return dbGrp
}

func CloseDB() {
	dbGrp.Lock()
	defer dbGrp.Unlock()

	if dbGrp.db1 != nil {
		lk.FailOnErr("%v", dbGrp.db1.Close())
		dbGrp.db1 = nil
	}
	if dbGrp.db2 != nil {
		lk.FailOnErr("%v", dbGrp.db2.Close())
		dbGrp.db2 = nil
	}
}
