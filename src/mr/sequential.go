package mr

// simple sequential MapReduce.
// go run sequential.go

import (
	"6.824/models"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
)

// ByKey for sorting by key.
type ByKey []models.KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Sequential(mrAppName string) {
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	mrApp := CreateMapReduceApp(mrAppName)

	var intermediate []models.KeyValue

	err := filepath.WalkDir("data", func(path string, file os.DirEntry, err error) error {
		if file.IsDir() {
			return nil
		}

		content, err := ioutil.ReadFile(path)
		if err != nil {
			log.Fatalf("cannot read %s, %v", file.Name(), err)
		}

		kva := mrApp.Map(file.Name(), string(content))
		intermediate = append(intermediate, kva...)

		return nil
	})

	if err != nil {
		panic(err)
	}

	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.

	sort.Sort(ByKey(intermediate))

	outputName := "mr-out-0"
	outputFile, _ := os.Create(outputName)

	//goland:noinspection GoUnhandledErrorResult
	defer outputFile.Close()

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := mrApp.Reduce(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, _ = fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}
