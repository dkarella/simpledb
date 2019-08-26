package simpledb

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

type index struct {
	table map[string]int64
	file  *os.File
}

func recover(f *os.File) (map[string]int64, error) {
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	m := make(map[string]int64)
	for _, token := range strings.Split(string(b), ",") {
		pair := strings.Split(token, ":")
		if len(pair) < 2 {
			continue
		}

		k := pair[0]
		v, err := strconv.Atoi(pair[1])
		if err != nil {
			return nil, err
		}

		m[k] = int64(v)
	}

	return m, nil
}

func createIndex(filename string) (*index, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}

		return &index{
			file:  file,
			table: make(map[string]int64),
		}, nil
	}

	table, err := recover(file)
	if err != nil {
		return nil, err
	}

	return &index{table, file}, nil
}

func (i *index) close() {
	i.file.Close()
}

func (i *index) put(k string, v int64) error {
	_, err := i.file.WriteString(fmt.Sprintf("%s:%d,", k, v))
	if err != nil {
		return err
	}

	i.table[k] = v

	return nil
}

func (i *index) get(k string) (int64, bool) {
	v, ok := i.table[k]
	return v, ok
}
