package mendb_test

import (
	"encoding/json"
	"testing"
)

func mock_data() map[string][]byte {
	m := map[string]interface{}{
		"key1": "hello world",
		"key2": []int{2, 5, 6},
		"key3": []string{"abc", "fyg"},
		"key4": map[string]int{"a": 1, "b": 2, "c": 6},
		"key5": "",
	}

	data := make(map[string][]byte)
	for k, v := range m {
		b, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}

		data[k] = b
	}

	return data
}

func TestNewSSTable(t *testing.T) {
	mock_data()
	// testDir := "test_data"
	// if _, err := os.Stat(testDir); os.IsNotExist(err) {
	// 	os.MkdirAll(testDir, 0766)
	// }
	// defer func() {
	// 	os.RemoveAll(testDir)
	// }()

	// table, err := mendb.NewSSTable(filepath.Join(testDir, "mendb0"))
	// if err != nil {
	// 	t.Error(err)
	// }

	// data := mock_data()
	// for k, v := range data {
	// 	if start, err := table.Set(k, v); err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	expected := int64(len(v)) + offset
	// 	assert.Equal(t, expected, table.OffSet)
	// }
}
