package utils

import (
	"crypto/rand"
	"encoding/hex"
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
	return hex.EncodeToString(bytes)
}

func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
