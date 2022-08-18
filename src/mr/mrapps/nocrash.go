package mrapps

//
// same as crash.go but doesn't actually crash.
//
// go build -buildmode=plugin nocrash.go
//

import (
	"6.824/models"
)
import crand "crypto/rand"
import "math/big"
import "strings"
import "os"
import "sort"
import "strconv"

type NoCrash struct {
}

func mayCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 500 {
		// crash!
		os.Exit(1)
	}
}

func (NoCrash) Map(filename string, contents string) []models.KeyValue {
	mayCrash()

	var kva []models.KeyValue
	kva = append(kva, models.KeyValue{Key: "a", Value: filename})
	kva = append(kva, models.KeyValue{Key: "b", Value: strconv.Itoa(len(filename))})
	kva = append(kva, models.KeyValue{Key: "c", Value: strconv.Itoa(len(contents))})
	kva = append(kva, models.KeyValue{Key: "d", Value: "xyzzy"})
	return kva
}

func (NoCrash) Reduce(key string, values []string) string {
	mayCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
