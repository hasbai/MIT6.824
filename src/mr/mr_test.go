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

func init() {
	// generate the correct output
	Sequential()
}

func Md5(data []byte) string {
	return fmt.Sprintf("%x", md5.Sum(data))
}

func TestWordCount(t *testing.T) {
	MakeCoordinator(10)
	time.Sleep(time.Second) // give the coordinator time to create the sockets.

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ { // start multiple workers.
		wg.Add(1)
		go func() {
			Worker("wc")
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
				slice := strings.Split(fileScanner.Text(), " ")
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
	expected, _ := os.ReadFile(expectedFileName)
	if Md5(buffer.Bytes()) != Md5(expected) {
		t.Error("files aren't equivalent")
	}

	// remove files
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
