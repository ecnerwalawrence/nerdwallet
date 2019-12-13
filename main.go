package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/go-pg/pg/v9"
)

// Randomize Success or Failure on asynchronous thread
var src = rand.NewSource(time.Now().UnixNano())
var threadResultRandomizer = rand.New(src)

/**
*  NOTE: I would use transactions to ensure consistency.
*        However, I did not have the time to figure out
*        the transaction functionality of this library
 */
const (
	QueueState   = "queue"
	HiddenState  = "hidden" // worker is processing task
	SuccessState = "success"
	FailureState = "failure"

	SizeOfBulk = 4
)

// Task - model for task
type Task struct {
	ID       int
	ExecTime time.Time
	TaskType string
	State    string // Const above are valid states
}

// TaskCreate - adds task into our datastore
func TaskCreate(db *pg.DB, d time.Time, taskType string) error {
	t := &Task{
		State:    QueueState,
		TaskType: taskType,
		ExecTime: d,
	}
	return db.Insert(t)
}

// TaskFind - model function for task
func TaskFind(db *pg.DB) (*[]Task, error) {
	var tasks []Task
	err := db.Model(&tasks).Where("state='queue' AND exec_time < CURRENT_TIMESTAMP").Select()
	if err != nil {
		return nil, err
	}
	return &tasks, err
}

// TaskBulkUpdateState - helper function
func TaskBulkUpdateState(db *pg.DB, tasksPtr *[]Task, state string) error {
	tasks := *tasksPtr
	uTasks := []Task{}
	for i := 0; i < len(tasks); i++ {
		uTasks = append(uTasks, Task{
			ID:    tasks[i].ID,
			State: state,
		})
	}
	_, err := db.Model(&uTasks).Column("state").Update()
	if err != nil {
		return err
	}
	return nil
}

// End of Task Model Functions

// Queue - library to schedule a task in the future
type Queue struct {
	DB *pg.DB
}

// Initialize - initialize datastore
func (q *Queue) Initialize() {
	q.DB = pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "nerdwallet",
	})
}

// Close - closes datastore
func (q *Queue) Close() {
	if q.DB != nil {
		q.DB.Close()
	}
}

// AddTask
func (q *Queue) AddTask(d time.Time, taskType string) error {
	return TaskCreate(q.DB, d, taskType)
}

// RunSchedule - execute tasks
//   Read DB
//   Runs the queries
//   Execute the task
func (q *Queue) RunSchedule() error {

	for {
		// Read DB and current to run a this time
		tasks, err := TaskFind(q.DB)
		if err != nil {
			fmt.Println("ERROR:", err)
			break
		}
		if tasks == nil || len(*tasks) == 0 {
			// Wait...
			fmt.Println("INFO: Nothing to do")
			time.Sleep(time.Duration(2) * time.Minute)
			continue
		}

		// Update records as "hidden"
		err = TaskBulkUpdateState(q.DB, tasks, HiddenState)
		if err != nil {
			fmt.Println("update hidden state error:", err)
			break
		}

		// Split the tasks into bulk. Hard-code to 4 for now
		bulkTasks := [][]Task{}
		bIndex := -1
		for i, t := range *tasks {
			if i%4 == 0 {
				bulkTasks = append(bulkTasks, []Task{})
				bIndex++
			}
			bulkTasks[bIndex] = append(bulkTasks[bIndex], t)
			fmt.Println("run:", i, t.ID)
		}

		// Channels - convince memory to safely pass data
		//            through goroutines.
		//            It is buffer so the goroutinue can quickly free
		//            itself and avoid too much context switching.
		successChan := make(chan Task, 4)
		errorChan := make(chan Task, 4)
		for _, bulk := range bulkTasks {
			go q.BulkRunner(bulk, successChan, errorChan)
		}

		// Wait for the GoRoutines to finish and organize
		// the tasks between failure and success
		successTasks := []Task{}
		errorTasks := []Task{}
		for _ = range *tasks {
			select {
			case s := <-successChan:
				fmt.Println("Success received", s.ID)
				successTasks = append(successTasks, s)
			case e := <-errorChan:
				fmt.Println("Error received", e.ID)
				errorTasks = append(errorTasks, e)
			}
		}

		// Update records as "success"
		err = TaskBulkUpdateState(q.DB, &successTasks, SuccessState)
		if err != nil {
			fmt.Println("update success state error:", err)
			break
		}
		// Update records as "failures"
		err = TaskBulkUpdateState(q.DB, &errorTasks, FailureState)
		if err != nil {
			fmt.Println("update failure state error:", err)
			break
		}
	}

	return nil
}

// BulkRunner - bulk task runner that is threaded by go
func (q *Queue) BulkRunner(bulkGo []Task, successChan, errorChan chan<- Task) {
	for _, t := range bulkGo {
		// Fake action by sleeping
		time.Sleep(time.Second)

		// Mark success or failure randomly
		//  - For simulating purposes, I am ignoring being thread-safe
		if threadResultRandomizer.Intn(2) != 0 {
			fmt.Println("WORKER EXECUTION (SUCCESS) ID:", t.ID)
			t.State = SuccessState
			successChan <- t
		} else {
			fmt.Println("WORKER EXECUTION (FAILURE) ID:", t.ID)
			t.State = FailureState
			errorChan <- t
		}
	}
}

func main() {
	fmt.Println("full testing")

	queue := Queue{}
	queue.Initialize()
	defer queue.Close()

	// You can modify this for variable times.
	queue.AddTask(time.Now().Add(time.Duration(-10)*time.Hour), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-20)*time.Hour), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-25)*time.Hour), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-30)*time.Hour), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-40)*time.Hour), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-50)*time.Minute), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-60)*time.Minute), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-70)*time.Minute), "sendemail")
	queue.AddTask(time.Now().Add(time.Duration(-90)*time.Minute), "sendemail")
	queue.AddTask(time.Now(), "sendemail2")
	queue.AddTask(time.Now(), "createUser")
	// End of variation

	queue.RunSchedule()
}
