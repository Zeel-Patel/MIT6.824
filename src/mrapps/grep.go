package main

import (
	"6.5840/mr"
	"strconv"
	"strings"
)

func Map(filename string, contents string) []mr.KeyValue {
	// function to split contents into an array of lines.
	ff := func(r rune) bool { return r == '\n' }
	lines := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, line := range lines {
		// if line contains the search string, emit it with a value of 1
		if strings.Contains(line, "And") {
			kv := mr.KeyValue{filename, "1"}
			kva = append(kva, kv)
		}
	}
	return kva
}

func Reduce(key string, values []string) string {
	// return the total number of occurrences of the search string across all files.
	return strconv.Itoa(len(values))
}
