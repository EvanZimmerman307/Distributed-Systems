package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// StringReverse()

	for {
		input := FetchTaskArgs{}
		reply := FetchTaskReply{}

		ok := call("Coordinator.FetchTask", &input, &reply)
		if ok {

			if reply.ExitMSG == "done" {
				return
			}

			switch reply.Job.Type {
			case "map":

				//Notify Map tracker of start time
				jobStartArgs := JobStartArgs{
					StartTime: time.Now(),
					ID:        reply.Job.Id,
					Status:    "in-progress",
				}

				jobStartReply := JobStartReply{}

				ok := call("Coordinator.MapJobStart", &jobStartArgs, &jobStartReply)
				if !ok {
					log.Println("Failed to notify coordinator about job assignment")
				} else {
					//fmt.Printf("Successfully notified coordinator that job %d is finished\n", reply.Job.Id)
				}

				file, err := os.Open(reply.Job.FileName[0])
				if err != nil {
					log.Fatalf("cannot open %v", reply.Job.FileName)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Job.FileName)
				}
				file.Close()
				kva := mapf(reply.Job.FileName[0], string(content)) //intermediate keys
				//fmt.Printf("kva %v\n", kva)

				createBuckets(kva, reply.Job.NumbReduce, reply.Job.Id)
				reply.Job.State = "finished"

				jobDoneArgs := JobDoneArgs{
					Job: reply.Job,
				}

				jobDoneReply := JobDoneReply{}

				success := call("Coordinator.MapJobDone", &jobDoneArgs, &jobDoneReply)
				if !success {
					log.Println("Failed to notify coordinator about job completion")
				} else {
					//fmt.Printf("Successfully notified coordinator that job %d is finished\n", reply.Job.Id)
				}

			case "reduce":

				//Notify Reduce tracker of start time
				jobStartArgs := JobStartArgs{
					StartTime: time.Now(),
					ID:        reply.Job.Id,
					Status:    "in-progress",
				}

				jobStartReply := JobStartReply{}

				ok := call("Coordinator.ReduceJobStart", &jobStartArgs, &jobStartReply)
				if !ok {
					log.Println("Failed to notify coordinator about job assignment")
				} else {
					//fmt.Printf("Successfully notified coordinator that job %d is finished\n", reply.Job.Id)
				}

				//fmt.Printf("Successfully received reduce job %d\n", reply.Job.Id)
				var kva []KeyValue                            //Macro array that stores all kv pairs for a reduce task
				for _, filename := range reply.Job.FileName { //loop through files for reduce task
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", reply.Job.FileName)
					}

					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						//err := dec.Decode(&kv)
						if err := dec.Decode(&kv); err != nil {
							break
						}
						//if err == io.EOF {
						//	break // End of file reached, exit the loop
						//} else if err != nil {
						//	log.Fatalf("error decoding JSON from file %v: %v", filename, err)
						//}
						kva = append(kva, kv) //add kv pairs from files to macro array
					}
				}

				sort.Sort(ByKey(kva)) //sort macro array
				//fmt.Printf("kva %v\n", kva)

				oname := fmt.Sprintf("mr-out-%d", reply.Job.Id)
				ofile, _ := os.Create(oname)

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				ofile.Close()

				reply.Job.State = "finished"

				jobDoneArgs := JobDoneArgs{
					Job: reply.Job,
				}

				jobDoneReply := JobDoneReply{}

				success := call("Coordinator.ReduceJobDone", &jobDoneArgs, &jobDoneReply)
				if !success {
					log.Println("Failed to notify coordinator about job completion")
				} else {
					//fmt.Printf("Successfully notified coordinator that job %d is finished\n", reply.Job.Id)
				}
			}
		} else {
			return

			//fmt.Printf("call failed!\n")

		}
	}

}

func createBuckets(kva []KeyValue, nReduce int, jobID int) error {
	// Separate key-value pairs into buckets
	buckets := make([][]KeyValue, nReduce)
	for _, keyvalue := range kva {
		bucketID := ihash(keyvalue.Key) % nReduce
		buckets[bucketID] = append(buckets[bucketID], keyvalue)
	}

	// Create intermediate files with buckets
	for i, bucket := range buckets {
		fileName := fmt.Sprintf("mr-%d-%d", jobID, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot create %v", fileName)
		}

		enc := json.NewEncoder(file)
		for _, keyvalue := range bucket {
			err := enc.Encode(&keyvalue)
			if err != nil {
				log.Fatalf("cannot encode %v", keyvalue)
			}
		}

		file.Close()
	}

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func StringReverse() {

	args := StringArg{}

	args.Input = "ABDCE"

	reply := StringReply{}

	ok := call("Coordinator.Reversal", &args, &reply)

	if ok {
		// reply.Y should be 100.
		fmt.Printf("Worker: %v\n", args.Input)
		fmt.Printf("Coordinator %v\n", reply.Output)
	} else {
		fmt.Printf("call failed!\n")
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
