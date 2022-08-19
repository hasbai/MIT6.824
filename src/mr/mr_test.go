package mr

import (
	"6.824/models"
	"bufio"
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const expectedFileName = "mr-out-0"

var workerNum = 3

func TestWordCount(t *testing.T) {
	mrApp := "wc"
	test(t, mrApp)
}

func TestIndexer(t *testing.T) {
	mrApp := "indexer"
	test(t, mrApp)
}

func TestMapParallelism(t *testing.T) {
	defer deleteFiles()
	mrApp := "m timing"
	run(mrApp)

	buffer := collectOutput()
	fmt.Println(buffer.String())

	parallelCnt := 0
	timesCnt := 0
	scanner := bufio.NewScanner(&buffer)
	for scanner.Scan() {
		slice := strings.Split(scanner.Text(), " ")
		if strings.HasPrefix(slice[0], "parallel") &&
			slice[1] == strconv.Itoa(workerNum) {
			parallelCnt++
		}
		if strings.HasPrefix(slice[0], "time") {
			timesCnt++
		}
	}
	assert.Equalf(t, timesCnt, workerNum, "worker nums incorrect")
	assert.Greaterf(t, parallelCnt, 0, "map workers did not run in parallel")
}

func TestReduceParallelism(t *testing.T) {
	defer deleteFiles()
	mrApp := "r timing"
	run(mrApp)

	var cnt int
	buffer := collectOutput()
	fmt.Println(buffer.String())

	scanner := bufio.NewScanner(&buffer)
	for scanner.Scan() {
		numS := strings.SplitN(scanner.Text(), " ", 2)[1]
		num, err := strconv.Atoi(numS)
		if err != nil {
			panic(err)
		}
		if num == workerNum {
			cnt++
		}
	}
	assert.Greaterf(t, cnt, 0, "too few parallel reduces")
}

func TestJobCount(t *testing.T) {
	defer deleteFiles()
	mrApp := "job count"
	run(mrApp)
	buffer := collectOutput()

	assert.Equalf(
		t,
		"8",
		strings.Split(strings.TrimSpace(buffer.String()), " ")[1],
		"job count is wrong",
	)
}

func TestEarlyExit(t *testing.T) {
	defer deleteFiles()
	mrApp := "early exit"

	MakeCoordinator(10)
	time.Sleep(time.Second)
	var (
		wg1 sync.WaitGroup // initial wait group
		wg2 sync.WaitGroup // final wait group
	)
	wg1.Add(1)                       // only add 1 to test early exit
	for i := 0; i < workerNum; i++ { // start multiple workers.
		wg2.Add(1)
		go func() {
			Worker(mrApp)
			wg1.Done()
			wg2.Done()
		}()
	}
	wg1.Wait()
	wg1.Add(workerNum) // prevent negative wait group count
	initialOutput := collectOutput()
	wg2.Wait()
	finalOutput := collectOutput()

	assert.Equalf(
		t,
		initialOutput.String(),
		finalOutput.String(),
		"output changed after first worker exited",
	)
}

func TestCrash(t *testing.T) {
	Sequential("no crash")

	mrApp := "crash"
	c := MakeCoordinator(10)
	time.Sleep(time.Second)
	for i := 0; i < workerNum; i++ { // start multiple workers.
		go infiniteWorker(mrApp)
	}
	for !c.Done() {
		time.Sleep(time.Second)
	}

	testEqual(t)
}

func infiniteWorker(mrApp string) {
	defer func() {
		go infiniteWorker(mrApp)
	}()
	Worker(mrApp)
}

func run(mrApp string) {
	MakeCoordinator(10)
	time.Sleep(time.Second) // give the coordinator time to create the sockets.

	var wg sync.WaitGroup
	for i := 0; i < workerNum; i++ { // start multiple workers.
		wg.Add(1)
		go func() {
			Worker(mrApp)
			wg.Done()
		}()
	}
	wg.Wait()
}

func test(t *testing.T, mrApp string) {
	Sequential(mrApp)
	run(mrApp)
	testEqual(t)
}

func testEqual(t *testing.T) {
	buffer := collectOutput()

	expectedData, err := os.ReadFile(expectedFileName)
	if err != nil {
		panic(err)
	}

	assert.Equalf(
		t,
		string(expectedData),
		buffer.String(),
		"files aren't equivalent",
	)

	deleteFiles()
}

func collectOutput() bytes.Buffer {
	kvs := make(ByKey, 0, 1000)

	files, err := filepath.Glob("mr-out-*")
	if err != nil {
		panic(err)
	}
	for _, filename := range files {
		if filename == expectedFileName {
			continue
		}
		file, _ := os.Open(filename)
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

	sort.Sort(kvs)

	var buffer bytes.Buffer
	for _, kv := range kvs {
		buffer.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
	}
	return buffer
}

func deleteFiles() {
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
