package Util

import (
	"bufio"
	"bytes"
	"container/heap"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"syscall"
)


func GetFilesInDirectory(dirName string) []map[string]int {
	// 01. Get the rlimit
	var rLimit syscall.Rlimit

	// contains array of filename, lastByte Read
	//[[file1: 2, file2: 3], [], ...]
	fileBucket := make([]map[string]int, 0)
	bucketSize := 100
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Print("Couldn't get the ULIMIT, falling back to 100 file buckets")
	} else {
		bucketSize = int(rLimit.Cur)
	}
	log.Printf("Bucket size set to: %d ", bucketSize)

	// 02. Use the bucketSize to construct buckets of files.
	// 02.a. Check if we can read the directory
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		log.Fatal("Couldn't stat " + dirName + ": " + err.Error())
	}
	temp := make(map[string]int)

	// 02.b. Get all the file names and put them into buckets
	for _, file := range files {
		temp[dirName +"/" +file.Name()] = 0
		if len(temp) == bucketSize {
			fileBucket = append(fileBucket, temp)
			temp = make(map[string]int)
		}
	}
	fileBucket = append(fileBucket, temp)
	log.Printf("bucketSize %d and files in bucket %d", len(fileBucket), len(fileBucket[0]))
	// provide the buckets
	return fileBucket
}

/**
 * Use bytes datatype to navigate the file for efficient reads
 * You can seek from the point you left off, if its a big file
 */
func readLine(fileName string, lastIndex int) (int, map[string]bool, error) {
	BUFSIZE := 2 * 1024 * 1024
	result := make(map[string]bool)
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	// 2MB buffer for each file
	buf := make([]byte, BUFSIZE)
	lineSep := []byte{'\n'}
	c, err := file.ReadAt(buf, int64(lastIndex))
	lastIndex = bytes.LastIndex(buf[:c], lineSep)
	lines := bytes.Split(buf[:lastIndex], lineSep)
	if lastIndex == 0 && c > lastIndex {
		lines = bytes.Split(buf[:c], lineSep)
	}
	if c <= BUFSIZE && lastIndex + 1 < c {
		lines = bytes.Split(buf[:c], lineSep)
	}
	for i := 0; i < len(lines); i++ {
		if len(bytes.TrimSpace(lines[i])) > 0 {
			result[string(lines[i])] = true
		}
	}
	if err != nil {
		return lastIndex, result, err
	}
	return lastIndex, result, nil
}

// Not an efficient way to do it
func readLineFromFile(fileName string, lineNumber int) (string, bool, int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Couldn't read from file %v", err.Error())
	}
	sc := bufio.NewScanner(file)
	lastLine := 0
	read := false
	for sc.Scan() {
		lastLine++
		if lastLine == lineNumber {
			temp := strings.TrimSpace(sc.Text())
			read = true
			return temp, read, lastLine
		}
	}
	defer file.Close()
	return sc.Text(), read, lastLine
}


func constructPriorityQueue(fileBucket map[string]int, bucketPQ *PriorityQueue) {
	filePQ := make(PriorityQueue, 0)
	heap.Init(&filePQ)
	var lastInserted string
	// keeping it less to 4000 sentences for now in the main PQ
	// Each file is 2KB, assuming ulimit is 10240 max, average sentence is 100 Bytes
	// 10240 * 2048 / 100 ~= 200,000 lines processed in a single heap
	// which equals a mem of 20 MB, should be able to process a minimum of 100 such buckets in a
	// 2 Gig  ram machine
	for ;len(*bucketPQ) < 200000 && len(fileBucket) != 0; {
		for fileName, line := range fileBucket {
			lastByte, temp, err := readLine(fileName, line)
			if len(temp) > 0 {
				for key, _ := range(temp) {
					line := &Node{Value: key}
					heap.Push(&filePQ, line)
				}
			}
			if err == io.EOF {
				delete(fileBucket, fileName)
			} else {
				fileBucket[fileName] = lastByte
			}
		}
	}
	// Transfer the contents from the file based Priority Queue to Buckets PQ
	for ;len(filePQ) > 0; {
		n := heap.Pop(&filePQ).(*Node)
		if len(lastInserted) == 0 {
			lastInserted = n.Value
			heap.Push(bucketPQ, n)
		} else {
			if lastInserted != n.Value {
				lastInserted = n.Value
				heap.Push(bucketPQ, n)
			}
		}
	}
}


func ConstructPriorityQueue(fileBucket map[string]int, dir string) *PriorityQueue {
	// 1. Construct the priorityQueue
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	constructPriorityQueue(fileBucket, &pq)
	return &pq
}