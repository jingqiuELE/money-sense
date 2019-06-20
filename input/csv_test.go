package input

import (
	"os"
	"strings"

	"reflect"
	"testing"

	"github.com/dinedal/textql/test_util"
)

var (
	simple = `a,b,c
1,2,3
4,5,6`

	simple_header = `TEXT,TEXT,TEXT
	t1,t2,t3`
	simple_header_nocol = `TEXT,TEXT,TEXT`
	bad                 = `a,b,c
1,2,
4,5,6
7,8


9,,10
11,12,13,14
"foo,bar","boo,\"far",","
'foo,bar','"','"'
"test
",multi-line
`
	bad_header = `TEXT,TEXT,TEXT`
)

func TestCSVInputFakesHeader(t *testing.T) {
	fp := test_util.OpenFileFromString(simple, "data.csv")
	defer fp.Close()
	defer os.Remove(fp.Name())

	fp_header := test_util.OpenFileFromString(simple_header_nocol, "header.csv")
	defer fp_header.Close()
	defer os.Remove(fp_header.Name())

	opts := &CSVInputOptions{
		Separator: ',',
		ReadFrom:  fp,
		Header:    fp_header,
	}

	input, _ := NewCSVInput(opts)
	expected := []string{"c0", "c1", "c2"}

	if !reflect.DeepEqual(input.columnNames(), expected) {
		t.Errorf("Header() = %v, want %v", input.columnNames(), expected)
	}
}

func TestCSVInputReadsHeader(t *testing.T) {
	fp := test_util.OpenFileFromString(simple, "data.csv")
	defer fp.Close()
	defer os.Remove(fp.Name())

	fp_header := test_util.OpenFileFromString(simple_header, "header.csv")
	defer fp_header.Close()
	defer os.Remove(fp_header.Name())

	opts := &CSVInputOptions{
		Separator: ',',
		ReadFrom:  fp,
		Header:    fp_header,
	}

	input, _ := NewCSVInput(opts)
	expected := []string{"t1", "t2", "t3"}

	if !reflect.DeepEqual(input.columnNames(), expected) {
		t.Errorf("Header() = %v, want %v", input.columnNames(), expected)
	}
}

func TestCSVInputReadsSimple(t *testing.T) {
	fp := test_util.OpenFileFromString(simple, "data.csv")
	defer fp.Close()
	defer os.Remove(fp.Name())

	fp_header := test_util.OpenFileFromString(simple_header, "header.csv")
	defer fp_header.Close()
	defer os.Remove(fp_header.Name())

	opts := &CSVInputOptions{
		Separator: ',',
		ReadFrom:  fp,
		Header:    fp_header,
	}

	input, _ := NewCSVInput(opts)
	expected := make([][]string, len(strings.Split(simple, "\n")))
	expected[0] = []string{"a", "b", "c"}
	expected[1] = []string{"1", "2", "3"}
	expected[2] = []string{"4", "5", "6"}

	for counter := 0; counter < len(expected); counter++ {
		row := input.ReadRecord()
		if !reflect.DeepEqual(row, expected[counter]) {
			t.Errorf("ReadRecord() = %v, want %v", row, expected[counter])
		}
	}
}

func TestCSVInputReadsBad(t *testing.T) {
	fp := test_util.OpenFileFromString(bad, "data.csv")
	defer fp.Close()
	defer os.Remove(fp.Name())

	fp_header := test_util.OpenFileFromString(bad_header, "header.csv")
	defer fp_header.Close()
	defer os.Remove(fp_header.Name())

	opts := &CSVInputOptions{
		Separator: ',',
		ReadFrom:  fp,
		Header:    fp_header,
	}

	input, _ := NewCSVInput(opts)
	expected := make([][]string, len(strings.Split(bad, "\n")))
	expected[0] = []string{"a", "b", "c"}
	expected[1] = []string{"1", "2", ""}
	expected[2] = []string{"4", "5", "6"}
	expected[3] = []string{"7", "8", ""}
	expected[4] = []string{"9", "", "10"}
	expected[5] = []string{"11", "12", "13", "14"}
	expected[6] = []string{"foo,bar", `boo,\"far`, ","}
	expected[7] = []string{`'foo`, `bar'`, `'"'`, `'"'`}
	expected[8] = []string{"test\n", "multi-line", ""}

	for counter := 0; counter < len(expected); counter++ {
		row := input.ReadRecord()
		if !reflect.DeepEqual(row, expected[counter]) {
			t.Errorf("ReadRecord() = %v, want %v", row, expected[counter])
		}
	}
}

func TestCSVInputHasAName(t *testing.T) {
	fp := test_util.OpenFileFromString(simple, "data.csv")
	defer fp.Close()
	defer os.Remove(fp.Name())

	fp_header := test_util.OpenFileFromString(simple_header, "header.csv")
	defer fp_header.Close()
	defer os.Remove(fp_header.Name())

	opts := &CSVInputOptions{
		Separator: ',',
		ReadFrom:  fp,
		Header:    fp_header,
	}

	input, _ := NewCSVInput(opts)
	expected := fp.Name()

	if !reflect.DeepEqual(input.Name(), expected) {
		t.Errorf("Name() = %v, want %v", input.Name(), expected)
	}
}
