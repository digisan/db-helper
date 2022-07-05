package example

import (
	"fmt"
	"testing"
)

func TestGetDB1(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	// new
	db1 := NewDB1("A")
	err := db1.AddData("1", "2")
	if err != nil {
		panic(err)
	}

	// load
	data, err := GetDB1Data("A")
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
}
