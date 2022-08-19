package mrapps

//
// a MapReduce pseudo-application that sometimes crashes,
// and sometimes takes a long time,
// to test MapReduces ability to recover.
//
// go build -buildmode=plugin crash.go
//

import (
	"6.824/models"
	crand "crypto/rand"
	"runtime"
)
import "math/big"
import "strings"
import "sort"
import "strconv"
import "time"

type Crash struct {
}

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 330 {
		// crash!
		runtime.Goexit()
	} else if rr.Int64() < 660 {
		// delay for a while.
		maxms := big.NewInt(10 * 1000)
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	}
}

func (Crash) Map(filename string, contents string) []models.KeyValue {
	maybeCrash()

	var kva []models.KeyValue
	kva = append(kva, models.KeyValue{Key: "a", Value: filename})
	kva = append(kva, models.KeyValue{Key: "b", Value: strconv.Itoa(len(filename))})
	kva = append(kva, models.KeyValue{Key: "c", Value: strconv.Itoa(len(contents))})
	kva = append(kva, models.KeyValue{Key: "d", Value: "xyzzy"})
	return kva
}

func (Crash) Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
