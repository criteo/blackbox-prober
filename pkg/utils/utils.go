package utils

import (
	"crypto/rand"
	"encoding/hex"
)

var (
	MetricSuffix           = "blackbox_prober"
	MetricHistogramBuckets = []float64{.00005, .0001, .00025, .0005, .00075, .001, .0025, .005, .010, .015, .020, .025, .030, .040, .050, .060, .075, .100, .250, .500, 1, 2.5, 5, 10, 15, 30, 45, 60}
)

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func RandomHex(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		PanicOnError(err)
	}
	return hex.EncodeToString(bytes)[0:n]
}

func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
