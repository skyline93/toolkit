package db

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"sync"
)

type SSTable struct {
	filePath string
	file     *os.File

	mu    sync.RWMutex
	index map[string]int64
}

func NewSSTable(filePath string) (*SSTable, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		filePath: filePath,
		file:     f,
		index:    make(map[string]int64),
	}, nil
}

func (s *SSTable) AddRow(key string, value interface{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k, err := s.encodeData(key)
	if err != nil {
		return err
	}

	v, err := s.encodeData(value)
	if err != nil {
		return err
	}

	var d []byte
	d = append(d, k...)
	d = append(d, v...)

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err = s.file.Write(d); err != nil {
		return err
	}

	s.index[key] = offset

	return nil
}

func (s *SSTable) encodeData(data interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(b)))

	var d []byte
	d = append(d, l...)
	d = append(d, b...)

	return d, nil
}

func (s *SSTable) ReadRow(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}

	offset := s.index[key]

	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	_, err = s.decodeData(f)
	if err != nil {
		return nil, err
	}

	v, err := s.decodeData(f)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (s *SSTable) decodeData(r io.Reader) ([]byte, error) {
	b := make([]byte, 8)

	if _, err := r.Read(b); err != nil {
		return nil, err
	}

	l := binary.LittleEndian.Uint64(b)

	d := make([]byte, l)
	if _, err := r.Read(d); err != nil {
		return nil, err
	}

	return d, nil
}
