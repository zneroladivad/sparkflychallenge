package main

import (
	"context"
	"encoding/csv"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

type safeMap struct {
	s   map[string]interface{}
	mut sync.Mutex
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan string)
	done := make(chan interface{})
	dir := "./Sparkfly-Challenge-TestData/testdata/"
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	//create map to check for duplicates
	dupMap := safeMap{
		s: make(map[string]interface{}),
	}
	// create a go routine for every file to check for duplicates. The number of routines is variable
	// ctx is to determine if the thread should still be run
	for range files {
		go codeCheck(in, done, &dupMap, ctx)
	}
	// create a waitgroup for the file loader functions in case no duplicate is found
	var wg sync.WaitGroup
	wg.Add(len(files))
	for _, file := range files {
		go inFile(dir+file.Name(), in, ctx, &wg)
	}
	// create a go routine to wait for all of the files to load
	// if all of the data has been loaded and sent to the channel, then there are no duplicates
	// let application know to close
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		log.Println("done loading all data and no duplicates found")
		var d interface{}
		done <- d
	}(&wg)

	//wait for either all of the data to load with no duplicates found or for codeCheck to have found
	//a duplicate
	select {
	case <-done:
		//log.Println("done sent")
		cancel()
	}
	//log.Println("all done")
}

//inFile reads a file and sends codes to be checked for duplicates
func inFile(file string, in chan string, ctx context.Context, wg *sync.WaitGroup) {
	//load the file
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	//range over the entire file, adding items to the channel to be consumed by codecheck, or
	//ending the thread if the context is no longer valid.
Loop:
	for i, line := range data {
		select {
		case <-ctx.Done():
			//kill the go routine if the context is no longer valid (if either a dupliccate has been found, or all of the input has been read)
			break Loop
		default:
			if i > 0 {
				in <- line[1]
			}
		}
	}
	wg.Done()
}

//codeCheck is a listener for codes, it then determines if there are duplicates in the batch
func codeCheck(in chan string, done chan interface{}, dupMap *safeMap, ctx context.Context) {
	// wait for channel input
Loop:
	for {
		select {

		case code := <-in:
			var x interface{}
			// map is not thread safe, and both reads and writes must be in sync to make sure
			// no writes occur at the time of a read. This ensures that every check for a duplicate
			// is valid
			dupMap.mut.Lock()
			if _, ok := dupMap.s[code]; ok {
				log.Println("found duplicate!  Abort Mission! ", code)
				var d interface{}
				done <- d
				dupMap.mut.Unlock()
				break Loop
			} else {
				dupMap.s[code] = x
			}
			dupMap.mut.Unlock()
		case <-ctx.Done():
			//kill the go routine if the context is no longer valid (if either a dupliccate has been found, or all of the input has been read)
			break Loop
		}
	}

}
