package example

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	mh "github.com/digisan/db-helper/mongo"
)

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
