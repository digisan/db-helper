package mongohelper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	jt "github.com/digisan/json-tool"
	lk "github.com/digisan/logkit"
	"go.mongodb.org/mongo-driver/bson"
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
	mtx *sync.Mutex
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
	if !jt.IsValid(data) {
		return nil, false, fmt.Errorf("invalid JSON")
	}
	return data, data[0] == '[', nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////

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

func Find[T any](rFilter io.Reader) (rt []*T, err error) {

	mtx.Lock()
	defer mtx.Unlock()

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

func FindOne[T any](rFilter io.Reader) (*T, error) {

	mtx.Lock()
	defer mtx.Unlock()

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

	one := new(T)
	if err := col.FindOne(Ctx, filter).Decode(one); err != nil {
		return nil, err
	}

	return one, nil
}

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
		return 0, nil, err
	}
	one := new(T)
	if err := result.Decode(one); err != nil {
		return 0, nil, err
	}
	return 1, one, nil

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

	objects, err := Find[T](rFilter)
	if err != nil {
		return 0, nil, err
	}
	result, err := col.DeleteMany(Ctx, filter)
	if err != nil {
		return 0, nil, err
	}
	return int(result.DeletedCount), objects, nil
}
