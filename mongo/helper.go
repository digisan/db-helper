package mongohelper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	dt "github.com/digisan/gotk/data-type"
	lk "github.com/digisan/logkit"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// ref https://blog.logrocket.com/how-to-use-mongodb-with-go/

const (
	// ping mongodb timeout
	timeout = 3
)

var (
	mtx = &sync.Mutex{}
	col *mongo.Collection
)

func pingMongoAsync(client *mongo.Client) chan error {
	cResult := make(chan error)
	go func() {
		cResult <- client.Ping(Ctx, readpref.Primary())
	}()
	return cResult
}

func getMongoClient(ip string, port int) *mongo.Client {
	if len(ip) == 0 {
		ip = "localhost"
	}
	if port == 0 {
		port = 27017
	}
	uri := fmt.Sprintf("mongodb://%s:%d", ip, port)
	client, err := mongo.Connect(Ctx, options.Client().ApplyURI(uri))
	lk.FailOnErr("Connect error: %v", err)

	select {
	case <-time.After(timeout * time.Second):
		lk.FailOnErr("ping mongodb error: %v", fmt.Errorf("timeout(%ds)", timeout))
		return nil
	case err := <-pingMongoAsync(client):
		lk.FailOnErr("ping mongodb error: %v", err)
		return client
	}
}

func UpdateMongoClient(ip string, port int) {
	mtx.Lock()
	defer mtx.Unlock()
	Client = getMongoClient(ip, port)
}

func UseDbCol(dbName, colName string) {
	mtx.Lock()
	defer mtx.Unlock()
	col = Client.Database(dbName).Collection(colName)
}

func DropDb(dbName string) error {
	mtx.Lock()
	defer mtx.Unlock()
	return Client.Database(dbName).Drop(Ctx)
}

func DropCol(dbName, colName string) error {
	mtx.Lock()
	defer mtx.Unlock()
	return Client.Database(dbName).Collection(colName).Drop(Ctx)
}

func DropCurrentCol() (int, error) {
	mtx.Lock()
	defer mtx.Unlock()
	if col == nil {
		return 0, nil
	}
	err := col.Drop(Ctx)
	if err == nil {
		col = nil
		return 1, nil
	}
	return 0, err
}

// return json string, is array type, error
func reader4json(r io.Reader) ([]byte, bool, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, false, err
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return []byte{}, false, nil
	}
	if !dt.IsJSON(data) {
		return nil, false, fmt.Errorf("invalid JSON")
	}
	return data, data[0] == '[', nil
}

func kv2json(key string, value any) string {
	js := ""
	switch value.(type) {
	case string:
		js = fmt.Sprintf(`{"%s": "%v"}`, key, value)
	default:
		js = fmt.Sprintf(`{"%s": %v}`, key, value)
	}
	lk.FailOnErrWhen(!dt.IsJSON([]byte(js)), "%v", fmt.Errorf("INVALID JSON"))
	return js
}

// return json string, for filter reader
func kv2reader(key string, value any) io.Reader {
	return strings.NewReader(kv2json(key, value))
}

func kvs2json(keys []string, values []any) string {
	if len(keys) == 0 || len(keys) != len(values) {
		lk.Warn("keys' length & values' length must be positive and identical")
		return ""
	}
	js := "{"
	for i, key := range keys {
		value := values[i]
		switch value.(type) {
		case string:
			js += fmt.Sprintf(`"%s": "%v",`, key, value)
		default:
			js += fmt.Sprintf(`"%s": %v,`, key, value)
		}
	}
	js = strings.TrimSuffix(js, ",") + "}"
	lk.FailOnErrWhen(!dt.IsJSON([]byte(js)), "%v", fmt.Errorf("INVALID JSON"))
	return js
}

func kvs2reader(keys []string, values []any) io.Reader {
	if js := kvs2json(keys, values); len(js) > 0 {
		return strings.NewReader(js)
	}
	return nil
}

// a should be primitive.A or []any type
func CvtA[T any](a any) ([]T, error) {
	if a == nil {
		return nil, nil
	}

	if arr, ok := a.(primitive.A); ok {
		rt := make([]T, 0, len(arr))
		for _, e := range arr {
			v, err := CvtM[T](e)
			if err != nil {
				return nil, err
			}
			rt = append(rt, *v)
		}
		return rt, nil
	}

	if arr, ok := a.([]any); ok {
		rt := make([]T, 0, len(arr))
		for _, e := range arr {
			v, err := CvtM[T](e)
			if err != nil {
				return nil, err
			}
			rt = append(rt, *v)
		}
		return rt, nil
	}

	lk.Warn("a @type [%T] should be added into CvtA", a)
	return []T{}, nil
}

// m should be primitive.M or map[string]any type
func CvtM[T any](m any) (*T, error) {
	if m == nil {
		return nil, nil
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	rt := new(T)
	if err := json.Unmarshal(data, rt); err != nil {
		return nil, err
	}
	return rt, nil
}

func json2bsonM(js string) bson.M {
	result := bson.M{}
	if err := bson.UnmarshalExtJSON([]byte(js), true, &result); err != nil {
		lk.Warn(err.Error())
		return nil
	}
	return result
}

func kv2bsonM(key string, value any) bson.M {
	return json2bsonM(kv2json(key, value))
}

func kvs2bsonM(keys []string, values []any) bson.M {
	return json2bsonM(kvs2json(keys, values))
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// return inserted id(s), inserted data
func Insert(rData io.Reader) (any, []byte, error) {

	mtx.Lock()
	defer mtx.Unlock()

	lk.FailOnErrWhen(col == nil, "%v", fmt.Errorf("collection is nil, use 'UseDbCol' to init one"))

	if rData == nil {
		return 0, []byte{}, nil
	}

	dataJSON, isArray, err := reader4json(rData)
	if err != nil {
		return nil, nil, err
	}

	if isArray {

		var docs []any
		err := bson.UnmarshalExtJSON(dataJSON, true, &docs)
		if err != nil {
			return nil, nil, err
		}
		result, err := col.InsertMany(Ctx, docs)
		if err != nil {
			return nil, nil, err
		}
		return result.InsertedIDs, dataJSON, nil

	} else {

		var doc any
		err := bson.UnmarshalExtJSON(dataJSON, true, &doc)
		if err != nil {
			return nil, nil, err
		}
		result, err := col.InsertOne(Ctx, doc)
		if err != nil {
			return nil, nil, err
		}
		return result.InsertedID, dataJSON, nil
	}
}

// filter: bson format
func find[T any](filter any) (rt []*T, err error) {
	cursor, err := col.Find(Ctx, filter)
	if err != nil {
		return nil, err
	}
	var results []bson.M
	if err = cursor.All(Ctx, &results); err != nil {
		return nil, err
	}
	for _, r := range results {
		data, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		one := new(T)
		err = json.Unmarshal(data, one)
		if err != nil {
			return nil, err
		}
		rt = append(rt, one)
	}
	return rt, nil
}

// return found objects
func Find[T any](rFilter io.Reader) (rt []*T, err error) {

	lk.FailOnErrWhen(col == nil, "%v", fmt.Errorf("collection is nil, use 'UseDbCol' to init one"))

	var filter any
	if rFilter != nil {
		filterJSON, _, err := reader4json(rFilter)
		if err != nil {
			return nil, err
		}
		if err := bson.UnmarshalExtJSON(filterJSON, true, &filter); err != nil {
			return nil, err
		}
	} else {
		filter = bson.D{}
	}
	return find[T](filter)
}

func FindAt[T any](field string, value any) (rt []*T, err error) {
	return Find[T](kv2reader(field, value))
}

// filter: bson format
func findOne[T any](filter any) (*T, error) {
	one := new(T)
	if err := col.FindOne(Ctx, filter).Decode(one); err != nil {
		if strings.Contains(err.Error(), "no documents in result") {
			return nil, nil
		}
		return nil, err
	}
	return one, nil
}

// return found object, if not found, return nil
func FindOne[T any](rFilter io.Reader) (*T, error) {

	lk.FailOnErrWhen(col == nil, "%v", fmt.Errorf("collection is nil, use 'UseDbCol' to init one"))

	var filter any
	if rFilter != nil {
		filterJSON, _, err := reader4json(rFilter)
		if err != nil {
			return nil, err
		}
		if err := bson.UnmarshalExtJSON(filterJSON, true, &filter); err != nil {
			return nil, err
		}
	} else {
		filter = bson.D{}
	}
	return findOne[T](filter)
}

func FindOneAt[T any](field string, value any) (*T, error) {
	return FindOne[T](kv2reader(field, value))
}

// return updated count
func Update(rFilter, rUpdate io.Reader, one bool) (int, error) {

	mtx.Lock()
	defer mtx.Unlock()

	lk.FailOnErrWhen(col == nil, "%v", fmt.Errorf("collection is nil, use 'UseDbCol' to init one"))

	var filter any
	if rFilter != nil {
		filterJSON, _, err := reader4json(rFilter)
		if err != nil {
			return 0, err
		}
		if err := bson.UnmarshalExtJSON(filterJSON, true, &filter); err != nil {
			return 0, err
		}
	} else {
		filter = bson.D{}
	}

	var update any
	if rUpdate != nil {
		updateJSON, _, err := reader4json(rUpdate)
		if err != nil {
			return 0, err
		}
		if err := bson.UnmarshalExtJSON(updateJSON, true, &update); err != nil {
			return 0, err
		}
	} else {
		return 0, nil
	}

	if one {
		result, err := col.UpdateOne(Ctx, filter, update)
		if err != nil {
			return 0, err
		}
		return int(result.ModifiedCount), nil
	} else {
		result, err := col.UpdateMany(Ctx, filter, update)
		if err != nil {
			return 0, err
		}
		return int(result.ModifiedCount), nil
	}
}

func UpdateAt(field string, value any, rUpdate io.Reader, one bool) (int, error) {
	return Update(kv2reader(field, value), rUpdate, one)
}

// return replaced count, after replacing data
func ReplaceOne(rFilter, rData io.Reader) (any, []byte, error) {

	mtx.Lock()
	defer mtx.Unlock()

	lk.FailOnErrWhen(col == nil, "%v", fmt.Errorf("collection is nil, use 'UseDbCol' to init one"))

	if rData == nil {
		return 0, []byte{}, nil
	}

	var filter any
	if rFilter != nil {
		filterJSON, _, err := reader4json(rFilter)
		if err != nil {
			return 0, nil, err
		}
		if err := bson.UnmarshalExtJSON(filterJSON, true, &filter); err != nil {
			return 0, nil, err
		}
	} else {
		filter = bson.D{}
	}

	dataJSON, _, err := reader4json(rData)
	if err != nil {
		return 0, nil, err
	}
	var doc any
	if err = bson.UnmarshalExtJSON(dataJSON, true, &doc); err != nil {
		return 0, nil, err
	}

	result, err := col.ReplaceOne(Ctx, filter, doc)
	if err != nil {
		return 0, nil, err
	}
	return result.ModifiedCount, dataJSON, nil
}

// if couldn't find, do nothing
func ReplaceOneAt(field string, value any, rData io.Reader) (any, []byte, error) {
	return ReplaceOne(kv2reader(field, value), rData)
}

// if inserted, return id,    inserted data
// if replaced, return count, after replacing data
func UpsertOneAt(field string, value any, rData io.Reader) (any, []byte, error) {

	object, err := FindOne[map[string]any](kv2reader(field, value))
	if err != nil {
		return nil, nil, err
	}

	if object == nil {
		return Insert(rData)
	}
	return ReplaceOne(kv2reader(field, value), rData)
}

// return deleted count, original object
func DeleteOne[T any](rFilter io.Reader) (int, *T, error) {

	mtx.Lock()
	defer mtx.Unlock()

	lk.FailOnErrWhen(col == nil, "%v", fmt.Errorf("collection is nil, use 'UseDbCol' to init one"))

	var filter any
	if rFilter != nil {
		filterJSON, _, err := reader4json(rFilter)
		if err != nil {
			return 0, nil, err
		}
		if err := bson.UnmarshalExtJSON(filterJSON, true, &filter); err != nil {
			return 0, nil, err
		}
	} else {
		return 0, nil, nil
	}

	result := col.FindOneAndDelete(Ctx, filter)
	if err := result.Err(); err != nil {
		if strings.Contains(err.Error(), "no documents in result") {
			return 0, nil, nil
		}
		return 0, nil, err
	}
	one := new(T)
	if err := result.Decode(one); err != nil {
		return 0, nil, err
	}
	return 1, one, nil

	// ERR: RE-READ !!!
	// object, err := FindOne[T](rFilter)
	// if err != nil {
	// 	return 0, nil, err
	// }
	// result, err := col.DeleteOne(Ctx, filter)
	// if err != nil {
	// 	return 0, nil, err
	// }
	// return int(result.DeletedCount), object, nil
}

func DeleteOneAt[T any](field string, value any) (int, *T, error) {
	return DeleteOne[T](kv2reader(field, value))
}

// return deleted count, original objects
func Delete[T any](rFilter io.Reader) (int, []*T, error) {

	mtx.Lock()
	defer mtx.Unlock()

	lk.FailOnErrWhen(col == nil, "%v", fmt.Errorf("collection is nil, use 'UseDbCol' to init one"))

	var filter any
	if rFilter != nil {
		filterJSON, _, err := reader4json(rFilter)
		if err != nil {
			return 0, nil, err
		}
		if err := bson.UnmarshalExtJSON(filterJSON, true, &filter); err != nil {
			return 0, nil, err
		}
	} else {
		return 0, nil, nil
	}

	objects, err := find[T](filter)
	if err != nil {
		return 0, nil, err
	}
	result, err := col.DeleteMany(Ctx, filter)
	if err != nil {
		return 0, nil, err
	}
	return int(result.DeletedCount), objects, nil
}

func DeleteAt[T any](field string, value any) (int, []*T, error) {
	return Delete[T](kv2reader(field, value))
}

func RemoveFields(field string, value any, remove ...string) error {
	if len(remove) == 0 {
		return errors.New("empty remove-fields, nothing to remove")
	}

	filter := kv2bsonM(field, value)

	ones := []any{}
	for range remove {
		ones = append(ones, 1)
	}
	unset := kvs2bsonM(remove, ones)
	update := bson.M{"$unset": unset}

	_, err := col.UpdateOne(Ctx, filter, update)
	return err
}
