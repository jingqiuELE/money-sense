package test_util

import (
	"io/ioutil"
	"os"
)

func OpenCSVFromString(contents string, filename string) *os.File {
	f, _ := ioutil.TempFile("./test/", filename)
	f.WriteString(contents)
	f.Seek(0, 0)
	return f
}
