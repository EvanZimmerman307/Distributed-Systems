package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

//import "io"

type job struct {
	Id         int
	Type       string
	FileName   []string
	NumbReduce int
	State      string
}

type MapTaskStatus struct {
	ID        int
	State     string
	StartTime time.Time
	Job       job
}

type ReduceTaskStatus struct {
	ID        int
	State     string
	StartTime time.Time
	Job       job
}

type Coordinator struct {
	// Your definitions here.
	// Jobs []job // should be a channel
	// You want a go version of a queue to store jobs
	Jobs        chan job
	MapTasks    []MapTaskStatus
	NReduce     int
	ReduceTasks []ReduceTaskStatus
	JobTimeout  time.Duration
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Reversal(args *StringArg, reply *StringReply) error {
	runes := []rune(args.Input)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	reply.Output = string(runes)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.AllReduceTasksFinished() {
		return true
	} else {
		return false
	}

	// Your code here.

	//return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// register tasks in coordinator structure - tasks will be fetched from the worker
	// coordinator manages the states of tasks

	c := Coordinator{
		Jobs:        make(chan job),
		MapTasks:    make([]MapTaskStatus, len(files)),
		NReduce:     nReduce,
		ReduceTasks: make([]ReduceTaskStatus, nReduce),
		JobTimeout:  10 * time.Second,
	}

	// Your code here.
	jobID := 0
	for _, file := range files {
		var mapFileArray [1]string
		mapFileArray[0] = file

		mapJob := job{
			Id:         jobID,
			Type:       "map",
			FileName:   mapFileArray[:],
			NumbReduce: nReduce,
			State:      "unfinished",
		}

		c.MapTasks[jobID] = MapTaskStatus{
			ID:    jobID,
			State: "unfinished",
			//StartTime: time.Now(),
			Job: mapJob,
		}

		jobID++

		go func(job job) {
			c.Jobs <- job
		}(mapJob)

	}

	var once sync.Once
	once.Do(func() {
		go c.monitorTimeouts() //fails early exit or crash sometimes
	})

	c.server()
	return &c
}

func (c *Coordinator) monitorTimeouts() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		if !c.AllMapTasksFinished() { //if the job is unassigned it could still be reassigned
			for _, task := range c.MapTasks {
				if task.State == "in-progress" && time.Since(task.StartTime) > c.JobTimeout {
					log.Printf("Map task %d timed out, reassigning\n", task.ID)
					task.State = "unfinished"
					task.Job.State = "unfinished"
					//task.StartTime = time.Now() // Update start time for the new assignment
					c.Jobs <- task.Job
				}
			}
		}

		if !c.AllReduceTasksFinished() {
			for _, task := range c.ReduceTasks {
				if task.State == "in-progress" && time.Since(task.StartTime) > c.JobTimeout {
					log.Printf("Reduce task %d timed out, reassigning\n", task.ID)
					task.State = "unfinished"
					task.Job.State = "unfinished"
					//task.StartTime = time.Now()
					c.Jobs <- task.Job
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	// get the task from the queue
	//mutex.lock?
	//job := <- c.Jobs
	//c.mu.Lock()
	//defer c.mu.Unlock()

	//tried monitortimeout() here, fails job count

	select {
	case job := <-c.Jobs:
		reply.Job = job
		reply.ExitMSG = ""
	default:
		//go c.monitorTimeouts()
		/* var once sync.Once
		once.Do(func() {
			go c.monitorTimeouts() //fails early exit or crash sometimes
		}) */

		if c.AllReduceTasksFinished() {
			reply.ExitMSG = "done"
			return nil
		}

		if c.AllMapTasksFinished() {
			//create reduce tasks
			c.CreateReduceTasks()
		}

	}
	return nil
}

func (c *Coordinator) MapJobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	// Logic to mark the job as finished, e.g., updating a data structure that tracks job states
	//c.Jobs <- args.Job
	c.MapTasks[args.Job.Id].State = args.Job.State
	return nil
}

func (c *Coordinator) ReduceJobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	// Logic to mark the job as finished, e.g., updating a data structure that tracks job states
	//c.Jobs <- args.Job
	c.ReduceTasks[args.Job.Id].State = args.Job.State
	return nil
}

func (c *Coordinator) MapJobStart(args *JobStartArgs, reply *JobStartReply) error {
	c.MapTasks[args.ID].StartTime = args.StartTime
	c.MapTasks[args.ID].State = args.Status
	return nil
}

func (c *Coordinator) ReduceJobStart(args *JobStartArgs, reply *JobStartReply) error {
	c.ReduceTasks[args.ID].StartTime = args.StartTime
	c.ReduceTasks[args.ID].State = args.Status
	return nil
}

func (c *Coordinator) AllMapTasksFinished() bool {
	for _, task := range c.MapTasks {
		if task.State != "finished" {
			return false
		}
	}
	return true
}

func (c *Coordinator) AllReduceTasksFinished() bool {
	for _, task := range c.ReduceTasks {
		if task.State != "finished" {
			return false
		}
	}
	return true
}

func (c *Coordinator) CreateReduceTasks() error {

	//jobID := 0
	for jobID := 0; jobID < c.NReduce; jobID++ {
		// get all the files that end with the corresponding job id
		filesForReduceTask := c.GetReduceTaskFiles(jobID)

		reduceJob := job{
			Id:         jobID,
			Type:       "reduce",
			FileName:   filesForReduceTask,
			NumbReduce: c.NReduce,
			State:      "unfinished",
		}

		c.ReduceTasks[jobID] = ReduceTaskStatus{
			ID:    jobID,
			State: "unfinished",
			//StartTime: time.Now(),
			Job: reduceJob,
		}

		go func(job job) {
			c.Jobs <- job
		}(reduceJob)
	}

	return nil
}

func (c *Coordinator) GetReduceTaskFiles(jobID int) []string {
	var filesForReduceTask []string

	// Get all files in the current directory.
	files, err := os.ReadDir(".")
	if err != nil {
		return nil
	}

	endingID := fmt.Sprintf("-%d", jobID)

	for _, file := range files {
		if file.IsDir() {
			continue // Skip directories
		}

		filename := file.Name()
		if strings.HasSuffix(filename, endingID) {
			filesForReduceTask = append(filesForReduceTask, filename)
		}
	}

	return filesForReduceTask

}

/***
Problems:
Job count test sometimes fails (9!=8)
Early exit sometimes fails
Crash usually passes

Tried: Use RPC to notify coordinator when worker is starting a job
***/
