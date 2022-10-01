package main

import (
	"compress/gzip"
	"io"
	"log"
	"os"
)

// YourSolution is an io.Reader that provides compressed bytes
type YourSolution interface {
	NewYourSolution(io.ReadCloser) io.Reader
}

type UploadManager struct {
	BufferSize int
}

// NewYourSolution takes a source of uncompressed bytes
func (u UploadManager) NewYourSolution(rc io.ReadCloser) io.Reader {
	/*instantiation code here */
	//create a buffer to read the input in chunks
	buffer := make([]byte, u.BufferSize)
	//create pipe to output to io.Reader for the S3
	pr, pw := io.Pipe()
	//create a gzip writer using the pipe
	gw := gzip.NewWriter(pw)
	//create a go routine to do the writing, immediately return the reader
	go func() {
		//copies the input from rc, into the gzip writer using the buffer
		n, err := io.CopyBuffer(gw, rc, buffer)
		//cleanup
		gw.Close()
		pw.Close()
		log.Printf("copied    %v %v", n, err)
	}()
	//return the io.Reader immediately
	return pr
}

func main() {
	// please enter the file name
	if len(os.Args) != 2 {
		log.Fatal("please enter the prograsm and then the filename on the root")
	}
	filename := os.Args[1]
	f, err := os.Open("./" + filename)
	if err != nil {

		log.Fatal(err)
	}
	defer f.Close()
	// specify buffer size for your needs.
	up := UploadManager{BufferSize: 4096}
	//implement as interface
	compress(up, f, filename)

}
func compress(ys YourSolution, readCloser io.ReadCloser, filename string) {
	mockS3(ys.NewYourSolution(readCloser), filename)
}

// mock to test the output.
func mockS3(compressedOutput io.Reader, filename string) error {
	outfile, err := os.Create(filename + ".gz")
	if err != nil {
		return err
	}
	defer outfile.Close()
	outfile.ReadFrom(compressedOutput)
	return nil
}
