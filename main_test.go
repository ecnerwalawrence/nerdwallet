package main

import (
	"testing"
	"time"

	"github.com/go-pg/pg/v9"
)

var testDB *pg.DB = initTestDB()

func initTestDB() *pg.DB {
	return pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "nerdwallet",
	})
}

func resetTestDB() {
	testDB.Exec("DELETE FROM tasks")
}

func findAllTaskTestDB() (*[]Task, error) {
	var tasks []Task
	err := testDB.Model(&tasks).Select()
	if err != nil {
		return nil, err
	}
	return &tasks, err
}

func TestTask(t *testing.T) {
	t.Run("TaskCreate", func(t *testing.T) {
		err := TaskCreate(testDB, time.Now(), "john")
		if err != nil {
			t.Errorf("Expected nil err but result:%v", err)
		}
	})
	resetTestDB()

	t.Run("TaskFind", func(t *testing.T) {
		execTime := time.Now().Add(time.Duration(-10) * time.Hour)
		err := TaskCreate(testDB, execTime, "joe")
		if err != nil {
			t.Errorf("Expected nil err, but result:%v", err)
		}
		tasksPtr, err := TaskFind(testDB)
		if err != nil || tasksPtr == nil {
			t.Errorf("Expected tasks to be not nil and not error result:%v", err)
		}

		if len(*tasksPtr) != 1 {
			t.Error("Expected to find one task")
		}

		tasks := *tasksPtr
		if tasks[0].TaskType != "joe" {
			t.Errorf("Expected to find 'joe', but result:%s", tasks[0].TaskType)
		}
	})
	resetTestDB()

	t.Run("TaskBulkUpdateState", func(t *testing.T) {
		execTime := time.Now().Add(time.Duration(-10) * time.Hour)
		err := TaskCreate(testDB, execTime, "mary")
		if err != nil {
			t.Errorf("Expected nil err but result:%v", err)
		}
		tasksPtr, err := TaskFind(testDB)
		if err != nil || tasksPtr == nil {
			t.Error("Expected tasks to be not nil and not error")
		}

		err = TaskBulkUpdateState(testDB, tasksPtr, "failure")
		if err != nil {
			t.Errorf("Expected tasks to be not nil and not error result:%v", err)
		}

		tasksPtr2, err := findAllTaskTestDB()
		if err != nil || tasksPtr == nil {
			t.Errorf("Expected tasks to be not nil and not error result:%v", err)
		}

		tasks2 := *tasksPtr2
		expectedState := "failure"
		if tasks2[0].State != expectedState {
			t.Errorf("Expected state to be '%s', but result:'%s'", expectedState, tasks2[0].State)
		}
	})
	resetTestDB()
}

func TestQueue(t *testing.T) {
	queue := Queue{DB: testDB}

	t.Run("AddTask", func(t *testing.T) {

		expectedTask := "sendemail"
		execTime := time.Now().Add(time.Duration(-10) * time.Hour)

		err := queue.AddTask(execTime, expectedTask)
		if err != nil {
			t.Error("Expected tasks to be not nil and not error")
		}

		tasksPtr, err := TaskFind(testDB)
		if err != nil || tasksPtr == nil {
			t.Error("Expected tasks to be not nil and not error")
		}

		tasks := *tasksPtr
		if tasks[0].TaskType != expectedTask {
			t.Errorf("Expected to find '%s', but result:'%s'", expectedTask, tasks[0].TaskType)
		}
	})
	resetTestDB()
}
