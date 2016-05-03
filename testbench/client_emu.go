package main

// import "github.com/ugorji/go/codec"
import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Row []interface{}

const numClients = 300
const secondsToRun = 10
const bufferSize = 100

type Payload struct {
	Checks   []Row `json:"checks"`
	SrcStore int   `json:"srcStore"`
}

func client(data chan Row, storeNum int, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Second * 1)
	for range ticker.C {
		checks := <-data
		if checks == nil {
			break
		}
		payload := Payload{
			Checks:   []Row{checks},
			SrcStore: storeNum,
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			log.Fatal(err)
			continue
		}
		body := bytes.NewReader(encoded)
		resp, err := http.Post("http://127.0.0.1:4567/pushJSON",
			"application/json",
			body)
		if err != nil {
			log.Fatal(err)
			continue
		}
		if resp.StatusCode != 200 {
			log.Fatal(resp.Status)
			continue
		}
	}
}

func checkIndexes() {
	resp, _ := http.Get("http://127.0.0.1:4567/checkIndexes")
	data, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(data))
}

func main() {
	var wg sync.WaitGroup
	wg.Add(numClients)
	data := make(chan Row, bufferSize)
	go fetcher(data, numClients*secondsToRun)
	for i := 0; i < numClients; i++ {
		go client(data, 1, &wg)
	}
	wg.Wait()
	checkIndexes()
	fmt.Println("Done")
}

func fetcher(output chan Row, rowNum int) {
	for i := 0; i < rowNum; i++ {
		row := Row{i, "hi" + strconv.Itoa(i)}
		output <- row
	}
	close(output)
}
