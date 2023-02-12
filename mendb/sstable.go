package mendb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type SSTable struct {
	mu      sync.RWMutex
	hashmap map[string]int64
	data    *os.File
	offset  int64
}

func NewSSTable(partPath string) (*SSTable, error) {
	fp, err := os.OpenFile(partPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	offset, err := fp.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		hashmap: make(map[string]int64),
		data:    fp,
		offset:  offset,
	}, nil
}

type Row struct {
	Key       string
	Value     any
	IsDeleted bool
	// Offset    int64
}

func (t *SSTable) EncodeRow(row *Row) ([]byte, error) {
	keyBuf := new(bytes.Buffer)
	err := binary.Write(keyBuf, binary.BigEndian, []byte(row.Key))
	if err != nil {
		return nil, err
	}

	valueBuf := new(bytes.Buffer)
	err = binary.Write(valueBuf, binary.BigEndian, row.Value)
	if err != nil {
		return nil, err
	}

	keyLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLen, uint64(len(keyBuf.Bytes())))

	valueLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLen, uint64(len(valueBuf.Bytes())))

	var firstByte byte = 1
	if row.IsDeleted {
		firstByte = 0
	}

	lineBuf := []byte{}
	lineBuf = append(lineBuf, firstByte)
	lineBuf = append(lineBuf, keyLen...)
	lineBuf = append(lineBuf, keyBuf.Bytes()...)
	lineBuf = append(lineBuf, valueLen...)
	lineBuf = append(lineBuf, valueBuf.Bytes()...)
	lineBuf = append(lineBuf, []byte("\n")...)

	return lineBuf, nil
}

func (t *SSTable) DecodeRow(v []byte) (*Row, error) {
	firstByte := v[0]

	var isDeleted bool
	if firstByte == 1 {
		isDeleted = true
	} else {
		isDeleted = false
	}

	keyLen := uint64(binary.LittleEndian.Uint32(v[1:8]))

	keyBuf := new(bytes.Buffer)

	a := v[9 : 9+keyLen]
	b := binary.BigEndian.Uint32(a)
	fmt.Printf("a: %v\n", b)

	err := binary.Read(keyBuf, binary.BigEndian, v[9:9+keyLen-1])
	if err != nil {
		return nil, err
	}

	a = v[9+keyLen+1 : 9+keyLen+1+8]
	fmt.Printf("a: %v\n", a)

	valueLen := binary.LittleEndian.Uint64(v[9+keyLen+1 : 9+keyLen+1+8])

	valueBuf := new(bytes.Buffer)
	err = binary.Read(valueBuf, binary.BigEndian, v[9+keyLen+8+1:9+keyLen+8+1+valueLen])
	if err != nil {
		return nil, err
	}

	return &Row{
		// Key:       keyBuf.String(),
		Value:     valueBuf.String(),
		IsDeleted: isDeleted,
	}, nil
}

// func (t *SSTable) encodeValue(v []byte) string {
// 	return hex.EncodeToString(v)
// }

// func (t *SSTable) decodeValue(v string) ([]byte, error) {
// 	return hex.DecodeString(v)
// }

// func (t *SSTable) joinLine(k string, v []byte) string {
// 	return k + ":" + t.encodeValue(v) + "\n"
// }

// func (t *SSTable) splitLine(line string) (string, []byte, error) {
// 	l := strings.Split(line, ":")

// 	k := l[0]
// 	v, err := t.decodeValue(l[1])

// 	return k, v, err
// }

// func (t *SSTable) set(key string, value []byte) (int64, error) {
// 	line := t.joinLine(key, value)

// 	n, err := t.data.WriteString(line)
// 	if err != nil {
// 		return 0, err
// 	}

// 	start := t.offset
// 	t.hashmap[key] = start

// 	next := start + int64(n)
// 	t.offset = next

// 	return start, nil
// }

// func (t *SSTable) Set(key string, value []byte) (int64, error) {
// 	t.mu.RLock()
// 	defer t.mu.RUnlock()

// 	return t.set(key, value)
// }

// func (t *SSTable) get(key string) ([]byte, error) {
// 	v, ok := t.hashmap[key]
// 	if ok {

// 	}
// }
