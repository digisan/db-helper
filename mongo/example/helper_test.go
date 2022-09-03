package example

import (
	"fmt"
	"os"
	"strings"
	"testing"

	mh "github.com/digisan/db-helper/mongo"
)

func TestInsert(t *testing.T) {
	mh.UseDbCol("testing", "users")

	////////////////////////////////////////////////////////

	r, err := os.Open("./s1.json")
	if err != nil {
		panic(err)
	}

	rID, err := mh.Insert(r)
	if err != nil {
		panic(err)
	}
	fmt.Println(rID)

	////////////////////////////////////////////////////////

	r, err = os.Open("./s2.json")
	if err != nil {
		panic(err)
	}

	rIDs, err := mh.Insert(r)
	if err != nil {
		panic(err)
	}
	fmt.Println(rIDs)
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

	rt, err := mh.Find[Person](strings.NewReader(`{
		"$and": [
			{
				"age": {
					"$gt": 60
				}
			}
		]
	}`))

	// rt, err := Find[Person](nil)

	if err != nil {
		panic(err)
	}

	// fmt.Println(rt)

	for _, p := range rt {
		fmt.Println()
		fmt.Print(p)
	}
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

	rt, err := mh.Delete(
		strings.NewReader(`{
			"age": {
				"$lt": 50
			}				
		}`),
		false,
	)

	if err != nil {
		panic(err)
	}

	fmt.Println(rt)
}
