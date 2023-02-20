package main

import (
	"fmt"

	"github.com/skyline93/toolkit/pkg/db"
)

func main() {
	t, err := db.NewSSTable("mlog")
	if err != nil {
		panic(err)
	}

	testdata := map[string]interface{}{
		"key1": "abc",
	}

	for k, v := range testdata {
		if err := t.AddRow(k, v); err != nil {
			panic(err)
		}
	}

	value, err := t.ReadRow("key1")
	if err != nil {
		panic(err)
	}

	fmt.Printf("value: %s\n", value)
}
