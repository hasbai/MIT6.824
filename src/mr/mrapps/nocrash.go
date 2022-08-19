package mrapps

//
// same as crash.go but doesn't actually crash.
//
// go build -buildmode=plugin noCrash.go
//

import (
	"6.824/models"
)
import "strings"
import "sort"
import "strconv"

type NoCrash struct {
}

func noCrash() {
	// no crash
}

func (NoCrash) Map(filename string, contents string) []models.KeyValue {
	noCrash()

	var kva []models.KeyValue
	kva = append(kva, models.KeyValue{Key: "a", Value: filename})
	kva = append(kva, models.KeyValue{Key: "b", Value: strconv.Itoa(len(filename))})
	kva = append(kva, models.KeyValue{Key: "c", Value: strconv.Itoa(len(contents))})
	kva = append(kva, models.KeyValue{Key: "d", Value: "xyzzy"})
	return kva
}

func (NoCrash) Reduce(key string, values []string) string {
	noCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
