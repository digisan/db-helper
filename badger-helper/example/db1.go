package example

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	bh "github.com/digisan/db-helper/badger-helper"
	. "github.com/digisan/go-generics/v2"
	lk "github.com/digisan/logkit"
)

const (
	SEP_K = "^"
	SEP_V = "^"
)

type DB1 struct {
	id        string
	data      []string
	fnDbStore func(*DB1) error
}

func NewDB1(id string) *DB1 {
	lk.FailOnErrWhen(strings.Contains(id, SEP_K), "%v", fmt.Errorf("invalid symbol(%s) in id", SEP_K))
	return &DB1{
		id:        id,
		data:      []string{},
		fnDbStore: bh.UpsertOneObjectDB[DB1],
	}
}

func (db1 DB1) String() string {
	sb := strings.Builder{}
	sb.WriteString("ID: " + db1.id + "\n")
	sb.WriteString("Data:")
	for _, item := range db1.data {
		sb.WriteString("\n  " + item)
	}
	return sb.String()
}

func (db1 *DB1) Key() []byte {
	return []byte(db1.id)
}

func (db1 *DB1) Marshal(at any) (forKey, forValue []byte) {
	forKey = db1.Key()
	lk.FailOnErrWhen(len(forKey) == 0, "%v", errors.New("invalid(empty) key for BadgerDB"))
	forValue = []byte(fmt.Sprint(db1.data))
	return
}

func (db1 *DB1) Unmarshal(dbKey, dbVal []byte) (any, error) {
	dbKeyStr := string(dbKey)
	typeid := strings.Split(dbKeyStr, SEP_K)
	db1.id = typeid[0]
	dbValStr := string(dbVal)
	dbValStr = strings.TrimPrefix(dbValStr, "[")
	dbValStr = strings.TrimSuffix(dbValStr, "]")
	db1.data = strings.Split(dbValStr, " ")
	db1.fnDbStore = bh.UpsertOneObjectDB[DB1]
	return db1, nil
}

func (db1 *DB1) BadgerDB() *badger.DB {
	return dbGrp.db1
}

func (db1 *DB1) AddData(items ...string) error {
	db1.data = append(db1.data, items...)
	db1.data = Settify(db1.data...)
	return db1.fnDbStore(db1)
}

func (db1 *DB1) RmData(items ...string) error {
	FilterFast(&db1.data, func(i int, e string) bool {
		return NotIn(e, items...)
	})
	return db1.fnDbStore(db1)
}

func GetDB1(id string) (*DB1, error) {
	db1, err := bh.GetOneObjectDB[DB1]([]byte(id))
	if err != nil {
		return nil, err
	}
	return db1, err
}

func GetDB1Data(id string) ([]string, error) {
	db1, err := bh.GetOneObjectDB[DB1]([]byte(id))
	if err != nil {
		return nil, err
	}
	if db1 == nil {
		return []string{}, nil
	}
	return db1.data, nil
}

func GetDB1s(prefix string, filter func(*DB1) bool) ([]*DB1, error) {
	db1s, err := bh.GetObjectsDB([]byte(prefix), filter)
	if err != nil {
		return nil, err
	}
	return db1s, err
}

func GetDB1First(prefix string, filter func(*DB1) bool) (*DB1, error) {
	return bh.GetFirstObjectDB([]byte(prefix), filter)
}

func GetDB1Count(prefix string, filter func(*DB1) bool) (int, error) {
	return bh.GetObjectCountDB([]byte(prefix), filter)
}
