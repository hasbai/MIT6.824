package mr

import (
	"6.824/models"
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const expectedFileName = "mr-out-0"

func Md5(data []byte) string {
	return fmt.Sprintf("%x", md5.Sum(data))
}

func TestWordCount(t *testing.T) {
	test(t, "wc")
}

func TestIndexer(t *testing.T) {
	test(t, "indexer")
}

func test(t *testing.T, mrApp string) {
	baseTest(t, mrApp, true)
}

func baseTest(t *testing.T, mrApp string, clear bool) {
	MakeCoordinator(10)
	time.Sleep(time.Second) // give the coordinator time to create the sockets.

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ { // start multiple workers.
		wg.Add(1)
		go func() {
			Worker(mrApp)
			wg.Done()
		}()
	}
	wg.Wait()

	kvs := make(ByKey, 0, 1000)
	_ = filepath.WalkDir(".", func(path string, fileInfo os.DirEntry, err error) error {
		filename := fileInfo.Name()
		if strings.HasPrefix(filename, "mr-out-") && filename != expectedFileName {
			file, _ := os.Open(path)
			fileScanner := bufio.NewScanner(file)
			for fileScanner.Scan() {
				slice := strings.SplitN(fileScanner.Text(), " ", 2)
				kvs = append(kvs, models.KeyValue{
					Key:   slice[0],
					Value: slice[1],
				})
			}
			_ = file.Close()
		}
		return nil
	})
	sort.Sort(kvs)

	var buffer bytes.Buffer
	for _, kv := range kvs {
		buffer.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
	}

	Sequential(mrApp)
	expectedData, err := os.ReadFile(expectedFileName)
	if err != nil {
		panic(err)
	}

	if Md5(buffer.Bytes()) != Md5(expectedData) {
		t.Error("files aren't equivalent")
	}

	if clear { // remove files
		files, err := filepath.Glob("mr-*")
		if err != nil {
			panic(err)
		}
		for _, f := range files {
			if err = os.Remove(f); err != nil {
				panic(err)
			}
		}
	}
}
