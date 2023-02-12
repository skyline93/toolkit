package main

import (
	"fmt"

	"github.com/skyline93/toolkit/mendb"
)

func main() {
	// msg := "hello world"

	// msgBuf := new(bytes.Buffer)
	// err := binary.Write(msgBuf, binary.BigEndian, []byte(msg))
	// if err != nil {
	// 	panic(err)
	// }

	// len := len(msgBuf.Bytes())
	// fmt.Printf("msg: %v\n", msgBuf.Bytes())

	// msgLen := make([]byte, 8)
	// binary.LittleEndian.PutUint64(msgLen, uint64(len))

	// fmt.Printf("msgLen: %v\n", msgLen)

	// lineBuf := []byte{}
	// lineBuf = append(lineBuf, 1)
	// lineBuf = append(lineBuf, msgLen...)
	// lineBuf = append(lineBuf, msgBuf.Bytes()...)
	// lineBuf = append(lineBuf, []byte("\n")...)

	// fmt.Printf("line: %v\n", lineBuf)

	t, err := mendb.NewSSTable("mendbLog")
	if err != nil {
		panic(err)
	}

	// d, err := t.JoinLine("key1", []byte("hello world"), false)

	// a := []int{2, 5, 6}
	// b := map[string]int{"a": 1, "b": 2, "c": 6}
	c := []byte("")

	// r, err := json.Marshal(c)
	// if err != nil {
	// 	panic(err)
	// }

	row := mendb.Row{
		Key:       "key1",
		Value:     c,
		IsDeleted: false,
	}
	d, err := t.EncodeRow(&row)

	// d, err := json.Marshal("hello world")
	if err != nil {
		panic(err)
	}

	fmt.Printf("d: %v\n", d)

	r, err := t.DecodeRow(d)
	if err != nil {
		panic(err)
	}

	fmt.Printf("r: %v\n", r)

	// start, err := t.Set("abc", d)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Printf("start: %d\n", start)
}
