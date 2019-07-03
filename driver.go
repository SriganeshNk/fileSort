package main

import (
	"Addepar/Util"
	"log"
	"os"
)

func remove(s []map[string]int, i int) []map[string]int {
	s[i] = s[len(s)-1]
	// We do not need to put s[i] at the end, as it will be discarded anyway
	return s[:len(s)-1]
}


func fileSortHandler(directory string, outFile string) {
	file, err := os.Create(outFile)
	defer file.Close()
	if err != nil {
		log.Fatalf("Can't open the file for writing, %v", err)
	}
	// Step1: to get the files in the directories
	fileBucket := Util.GetFilesInDirectory(directory)
	for key, val := range fileBucket {
		log.Println(key, val)
	}
	// Step2: Construct Priority Queue for each bucket
	pqs := make(Util.PriorityQueue, 0)
	temp := make([]*Util.PriorityQueue, 0)
	done := make([]int, 0)
	allDone := 0
	writeAt := 0
	// Initializing the PQs
	for i := 0; i < len(fileBucket); i++ {
		temp = append(temp, Util.ConstructPriorityQueue(fileBucket[i], directory))
		done = append(done, 0)
	}
	// TODO: Step3: push into the new file from each priorityQueue
	for len(fileBucket) != allDone  {
		for i := 0; i < len(fileBucket); i++ {
			if len(*temp[i]) == 0 && done[i] == 0 {
				temp[i] = Util.ConstructPriorityQueue(fileBucket[i], directory)
				if len(*temp[i]) == 0 {
					done[i] = 1
					allDone += 1
				}
			} else {
				n := temp[i].Pop().(*Util.Node)
				pqs.Push(n)
			}
		}
		if len(pqs) > 0 {
			res := pqs.Pop().(*Util.Node)
			n, err := file.WriteAt([]byte(res.Value+"\n"), int64(writeAt))
			writeAt += n
			if err != nil {
				log.Fatalf("something wrong with writing contents to file %v", err)
			}
		}
	}
	for len(pqs) > 0 {
		res := pqs.Pop().(*Util.Node)
		n, err := file.WriteAt([]byte(res.Value+"\n"), int64(writeAt))
		writeAt += n
		if err != nil {
			log.Fatalf("something wrong with writing contents to file %v", err)
		}
	}

	// Partial solution for now, using just one bucket.
	pq := Util.ConstructPriorityQueue(fileBucket[0], directory)
	for len(*pq) > 0 {
		node := pq.Pop().(*Util.Node)
		log.Print(node.Value)
	}
}


func main() {
	args := os.Args[1:]
	if len(args) != 2 {
		log.Fatal("Usage ./merge <input_dir> <output_file>")
	}
	directory := args[0]
	outFile := args[1]
	fileSortHandler(directory, outFile)
}
