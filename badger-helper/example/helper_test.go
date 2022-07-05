package example

import (
	"fmt"
	"testing"
)

func TestGetDB1(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	// new
	db1 := NewDB1("AB")
	err := db1.AddData("1", "2", "3")
	if err != nil {
		panic(err)
	}

	// load
	data, err := GetDB1Data("AB")
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
}

func TestGetDB1s(t *testing.T) {

	InitDB("./data")
	defer CloseDB()

	db1s, err := GetDB1s("A", func(d *DB1) bool { return len(d.id) == 2 })
	if err != nil {
		panic(err)
	}
	for _, db1 := range db1s {
		fmt.Println(db1)
	}

	fmt.Println("----------------------")

	db1, err := GetDB1First("A", func(d *DB1) bool { return len(d.id) == 2 })
	if err != nil {
		panic(err)
	}
	fmt.Println(db1)

	fmt.Println("----------------------")

	db1N, err := GetDB1Count("A", func(d *DB1) bool { return len(d.id) == 2 })
	if err != nil {
		panic(err)
	}
	fmt.Println(db1N)
}
