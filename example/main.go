package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/dkarella/simpledb"
)

func main() {
	db, err := simpledb.Connect("database")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("foo-%d", i)
		v := strconv.Itoa(int(time.Now().Unix()))
		db.Put(k, []byte(v))
	}

	v, _ := db.Get("foo-0")
	if _, err := db.Get("dne"); err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(v))
}
