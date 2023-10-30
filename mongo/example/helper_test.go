package example

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	mh "github.com/digisan/db-helper/mongo"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// https://www.mongodb.com/docs/drivers/go/current/fundamentals/crud/read-operations/query-document/#std-label-golang-literal-values
// "$eq" "$lt" "$gt" "$lte"
// "$exists"
// "$regex"
// "$all"
// "$bitsAllSet"

func TestDrop(t *testing.T) {
	mh.UseDbCol("dictionaryTest", "pathval")
	fmt.Println(mh.DropCurrentCol())
}

func TestInsert(t *testing.T) {
	mh.UseDbCol("testing", "users")

	////////////////////////////////////////////////////////

	r, err := os.Open("./s1.json")
	if err != nil {
		panic(err)
	}

	rID, data, err := mh.Insert(r)
	if err != nil {
		panic(err)
	}
	fmt.Println(rID)
	fmt.Println(string(data))

	////////////////////////////////////////////////////////

	r, err = os.Open("./s2.json")
	if err != nil {
		panic(err)
	}

	rIDs, data, err := mh.Insert(r)
	if err != nil {
		panic(err)
	}
	fmt.Println(rIDs)
	fmt.Println(string(data))

	fmt.Println(time.Now())
}

func TestFind(t *testing.T) {
	mh.UseDbCol("testing", "users")

	////////////////////////////////////////////////////////

	// retrieve single and multiple documents with a specified filter using FindOne() and Find()
	// create a search filer
	// filter := bson.D{
	// 	{
	// 		"$and",
	// 		bson.A{
	// 			bson.D{
	// 				{
	// 					"age",
	// 					bson.D{{"$gt", 25}},
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	rt, err := mh.FindOne[Person](strings.NewReader(`{
		"$and": [
			{
				"age": {
					"$gt": 600
				}
			}
		]
	}`))

	// rt, err := Find[Person](nil)

	if err != nil {
		panic(err)
	}

	fmt.Println(rt)

	// for _, p := range rt {
	// 	fmt.Println()
	// 	fmt.Print(p)
	// }
}

func TestUpdate(t *testing.T) {
	mh.UseDbCol("testing", "users")

	rt, err := mh.Update(
		strings.NewReader(`{
			"$and": [
				{
					"age": {
						"$gt": 60
					}
				}
			]
		}`),
		// nil,
		strings.NewReader(`{
			"$set": {
				"fullName": "User Modified"
			},
			"$inc": {
				"age": 1
			}
		}`),
		false,
	)

	if err != nil {
		panic(err)
	}

	fmt.Println(rt)
}

func TestDelete(t *testing.T) {
	mh.UseDbCol("testing", "users")

	rt, p, err := mh.Delete[Person](
		strings.NewReader(`{
			"age": {
				"$lt": 50
			}				
		}`))

	if err != nil {
		panic(err)
	}

	fmt.Println(rt)
	fmt.Println(p)
}

func TestReplace(t *testing.T) {

	mh.UseDbCol("testing", "users")

	r, err := os.Open("./s1.json")
	if err != nil {
		panic(err)
	}

	id, data, err := mh.ReplaceOne(strings.NewReader(`{"age": 22}`), r)
	if err != nil {
		panic(err)
	}

	fmt.Println(id)
	fmt.Println(string(data))
	fmt.Println(time.Now())
	fmt.Print()
}

func TestUpsert(t *testing.T) {

	mh.UseDbCol("testing", "users")

	r, err := os.Open("./s3.json")
	if err != nil {
		panic(err)
	}

	result, data, err := mh.Upsert(r, "age", 11)
	if err != nil {
		panic(err)
	}

	fmt.Println(result)
	fmt.Println(string(data))
	fmt.Println(time.Now())
	fmt.Print("   ")
}

func TestDelete2(t *testing.T) {

	sFilter := fmt.Sprintf(`{"Entity": "%v"}`, "My Test 1")

	mh.UseDbCol("dictionaryTest", "entities")
	n, _, err := mh.DeleteOne[any](strings.NewReader(sFilter))
	fmt.Println("1:", n, err)

	mh.UseDbCol("dictionaryTest", "entities_text")
	n, _, err = mh.DeleteOne[any](strings.NewReader(sFilter))
	fmt.Println("2:", n, err)
}

type P struct {
	A int
	B int
}

func (p P) String() string {
	return fmt.Sprintf("{A: %v  B: %v}", p.A, p.B)
}

func TestCvtAM(t *testing.T) {

	item1, item2 := new(P), new(P)
	item1.A = 1
	item2.A = 10
	a1 := primitive.A{
		item1, item2,
	}
	arr1, err1 := mh.CvtA[P](a1)
	fmt.Println(err1)
	fmt.Println(arr1)

	////////////////

	a2 := primitive.A{
		1, 2, 3,
	}
	arr2, err2 := mh.CvtA[int](a2)
	fmt.Println(err2)
	fmt.Println(arr2)

	fmt.Println("-------------------")

	///////////////////////////

	type class struct {
		A string `json:"a"`
		B string `json:"b"`
		C string `json:"c"`
	}

	m := primitive.M{
		"a": "AAA",
		"b": "BBB",
		"c": "CCC",
	}
	cls, err := mh.CvtM[class](m)
	fmt.Println(err)
	fmt.Println(cls.A, cls.B, cls.C)
	fmt.Println(*cls)
}

func TestRemoveField(t *testing.T) {

	// clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	// client, err := mongo.Connect(context.Background(), clientOptions)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer client.Disconnect(context.Background())

	// collection := client.Database("MyDictionaryV2").Collection("act_delete")

	// filter := bson.M{"User": "qmiao"}
	// update := bson.M{"$unset": bson.M{"Did": 1, "Action": 1}}

	// result, err := collection.UpdateOne(context.Background(), filter, update)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// fmt.Println(result.UpsertedCount)

	/////////////////////////////////////////////////////////////////

	mh.UseDbCol("MyDictionaryV2", "entities")

	err := mh.RemoveFields("Entity", "Language spoken at home", "SIF", "Collections", "Metadata")
	if err != nil {
		fmt.Println(err)
	}
}
