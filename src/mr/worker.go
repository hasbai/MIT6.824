package mr

import (
	"6.824/models"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "hash/fnv"

// use iHash(key) % NReduce to choose reduce task number for each KeyValue emitted by Map.
func iHash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by main.go worker
func Worker(mrAppName string) {
	mrApp := CreateMapReduceApp(mrAppName)

	// Your worker implementation here.

	uid := uuid.NewString()
	log.Printf("worker %s spawn, app %s", uid, mrAppName)
	defer log.Printf("worker %s exit", uid)

	for {
		task := getTask(uid)
		if task == emptyTask {
			break
		}
		if task.Code == TaskCodeWait {
			log.Printf("task %d needs waiting, sleep for a while...", task.ID)
			time.Sleep(time.Second * 1)
			continue
		}

		log.Printf("worker %s running task %d", uid, task.ID)
		err := runTask(task, mrApp)
		if err != nil {
			log.Printf("task %d failed, %v", task.ID, err)
			return
		}

		log.Printf("task %d finished", task.ID)
	}
	log.Printf("all done")
}

func getTask(workerID string) Task {
	task := Task{}
	args := GetTaskArgs{WorkerID: workerID}
	ok := call("Coordinator.GetTask", &args, &task)
	if !ok {
		log.Println("get task failed")
	}
	return task
}

func runTask(task Task, app MapReduce) error {
	switch task.Type {
	case TaskTypeMap:
		return runMap(task, app.Map)
	case TaskTypeReduce:
		return runReduce(task, app.Reduce)
	default:
		panic("unknown task type")
	}
}

func runMap(task Task, mapFunc MapFunc) error {
	content, err := os.ReadFile(task.FilePath)
	if err != nil {
		return err
	}
	kvs := mapFunc(
		path.Base(strings.Replace(task.FilePath, "\\", "/", -1)),
		string(content),
	)
	tmpFiles := make([]tmpFileStruct, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		tmpFile := bytes.NewBuffer([]byte{})
		tmpFiles[i].filename = fmt.Sprintf("mr-%d-%d", task.ID, i)
		tmpFiles[i].buffer = tmpFile
		tmpFiles[i].encoder = json.NewEncoder(tmpFile)
	}

	for _, kv := range kvs {
		index := iHash(kv.Key) % task.NReduce
		err = tmpFiles[index].encoder.Encode(&kv)
		if err != nil {
			return err
		}
	}

	for _, tmpFile := range tmpFiles {
		err = os.WriteFile(tmpFile.filename, tmpFile.buffer.Bytes(), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func runReduce(task Task, reduceFunc ReduceFunc) error {
	kvs := make([]models.KeyValue, 0, 1000)
	files, err := filepath.Glob("mr-*")
	if err != nil {
		panic(err)
	}
	for _, filename := range files {
		if !isIntermediateFile(filename, task.ID) {
			continue
		}

		var file *os.File
		file, err = os.Open(filename)
		if err != nil {
			return err
		}

		dec := json.NewDecoder(file)
		for {
			var kv models.KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		_ = file.Close()

		//err = os.Remove(filename)
		//if err != nil {
		//	return err
		//}
	}

	sort.Sort(ByKey(kvs))

	buffer := bytes.NewBuffer([]byte{})
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reduceFunc(kvs[i].Key, values)
		buffer.WriteString(fmt.Sprintf("%v %v\n", kvs[i].Key, output))
		i = j
	}

	return os.WriteFile(
		"mr-out-"+strconv.Itoa(task.ID+1), // id starts from 0
		buffer.Bytes(),
		0644,
	)
}

type tmpFileStruct struct {
	filename string
	buffer   *bytes.Buffer
	encoder  *json.Encoder
}

var re = regexp.MustCompile(`mr-(\d+)-(\d+)`)

func isIntermediateFile(filename string, reduceID int) bool {
	result := re.FindStringSubmatch(filename)
	if len(result) < 3 {
		return false
	}

	id, err := strconv.Atoi(result[2])
	if err != nil {
		return false
	}

	return id == reduceID
}
