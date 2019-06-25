package storage

import (
	"log"
	"time"
)

func ValString(values []string, types []string, timeFormat string) ([]string, error) {
	if len(types) != len(values) {
		log.Fatal("ValString can't handle unmatched types and values!")
	}
	result := make([]string, len(types))
	for i, tname := range types {
		switch tname {
		case "TIMESTAMP":
			vtime, err := time.Parse(time.RFC3339Nano, values[i])
			if err != nil {
				log.Fatal("Failed to parse time according to timeFormat:", timeFormat)
			}
			result[i] = vtime.Format(timeFormat)
		default:
			result[i] = values[i]
		}
	}
	return result, nil
}

func StringVal(values []string, types []string, timeFormat string) ([]interface{}, error) {
	if len(types) != len(values) {
		log.Fatal("StringVal can't handle unmatched types and values!")
	}
	var result []interface{}
	for i, tname := range types {
		switch tname {
		case "TIMESTAMP":
			vtime, err := time.Parse(timeFormat, values[i])
			if err != nil {
				log.Fatal("Failed to parse time according to timeFormat:", timeFormat)
			}
			result = append(result, vtime)
		default:
			result = append(result, values[i])
		}
	}
	return result, nil
}
