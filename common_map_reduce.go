package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func doMap(jobName string, mapTaskNumber int, inFile string, nReduce int, mapF func(file string, contents string) []KeyValue) {
	f, err := os.Open(inFile)

	defer f.Close()
	if err != nil {
		fmt.Print(err)
		return
	}

	fInfo, err := f.Stat()
	if err != nil {
		fmt.Print(err)
		return
	}

	fcontent := make([]byte, fInfo.Size())
	f.Read(fcontent)
	contents := string(fcontent)

	keyValue := mapF(inFile, contents)
	fJson := make([]*json.Encoder, nReduce)

	for i := range fJson {
		filename := reduceName(jobName, mapTaskNumber, i)
		file, err := os.Create(filename)

		if err != nil {
			fmt.Print(err)
			return
		}

		defer f.Close()
		fJson[i] = json.NewEncoder(file)
	}

	for _, kv := range keyValue {
		index := ihash(kv.Key) % nReduce
		fJson[index].Encode(&kv)
	}
}


func doReduce(jobName string, reduceTaskNumber int, nMap int, reduceF func(key string, values []string) string) {
	keyValue := make(map[string][]string)

	for i := 0 ; i < nMap ; i++ {
		filename := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(filename)

		if err != nil {
			fmt.Print(err)
			continue
		}

		jsonDecoder := json.NewDecoder(file)

		for {
			var kv KeyValue
			err_ := jsonDecoder.Decode(&kv)
			if err_ != nil {
				break
			}
			if _, ok := keyValue[kv.Key]; ok {
				keyValue[kv.Key] = append(keyValue[kv.Key], kv.Value)
			} else {
				newvalue := []string{kv.Value}
				keyValue[kv.Key] = newvalue

			}
		}

		file.Close()
	}

	file, err := os.Create(mergeName(jobName, reduceTaskNumber))
	defer file.Close()
	if err != nil {
		fmt.Print(err)
		return
	}

	keys := make([]string,0,len(keyValue))
	for k := range keyValue {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		fmt.Fprintf(file, "%v %v\n", k, reduceF(k, keyValue[k]))
	}
}

// merge combines the results of the many reduce jobs into a single output file
func doMerge(jobName string, nReduce int) {
	kvs := make(map[string]string)
	for i := 0; i < nReduce; i++ {
		p := mergeName(jobName, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create("mrtmp." + jobName)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%v %v\n", k, kvs[k])
	}

	w.Flush()
	file.Close()
}