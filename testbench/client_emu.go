package main

import "github.com/ugorji/go/codec"
import (
	"bytes"
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
const useCBOR = false
const jsonPushURL = "http://127.0.0.1:4567/pushJSON"
const cborPushURL = "http://127.0.0.1:4567/push"
const checkIndexesURL = "http://127.0.0.1:4567/checkIndexes"
const deleteAllURL = "http://127.0.0.1:4567/deleteAll"

type Payload struct {
	Checks   []Row `json:"checks"`
	SrcStore int   `json:"srcStore"`
}

func client(data chan Row, storeNum int, wg *sync.WaitGroup, cborEnabled bool) {
	defer wg.Done()
	var enc *codec.Encoder
	var url string
	var mimeType string
	buff := new(bytes.Buffer)
	if cborEnabled {
		enc = codec.NewEncoder(buff, new(codec.CborHandle))
		url = cborPushURL
		mimeType = "application/cbor"
	} else {
		enc = codec.NewEncoder(buff, new(codec.JsonHandle))
		url = jsonPushURL
		mimeType = "application/json"
	}

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
		err := enc.Encode(payload)
		if err != nil {
			log.Fatal(err)
			continue
		}
		tr := &http.Transport{MaxIdleConnsPerHost: 1}
		client := &http.Client{Transport: tr}
		resp, err := client.Post(url, mimeType, buff)
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
	resp, _ := http.Get(checkIndexesURL)
	data, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(data))
}

func deleteAll() {
	_, err := http.Post(deleteAllURL, "", nil)
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(numClients)
	data := make(chan Row, bufferSize)
	go fetcher(data, numClients*secondsToRun)
	deleteAll()
	for i := 0; i < numClients; i++ {
		go client(data, i, &wg, useCBOR)
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
