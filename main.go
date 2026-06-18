package main

import (
	"fmt"
	"sync/atomic"
	"time"
)


type FutureResult struct {
	Done       atomic.Bool
	ResultChan chan string
}

type Task func() string

func Async(t Task) *FutureResult {
	result := &FutureResult{
		Done: atomic.Bool{},
		ResultChan: make(chan string , 1),
	}

	go func ()  {
		result.ResultChan <- t()
		result.Done.Store(true)
	}()

	return result
}

func AsyncWithTimeout(t Task, timeout time.Duration) *FutureResult {
	result := &FutureResult{
		Done: atomic.Bool{},
		ResultChan: make(chan string , 1),
	}

	go func ()  {
		result.ResultChan <- t()
		result.Done.Store(true)
	}()

	go func ()  {
		select{
			case <-time.After(timeout) :
				result.ResultChan <- "timeout"
		}	
	}()

	return result
}

func (fResult *FutureResult) Await() string {
	
	return <-fResult.ResultChan

}

func CombineFutureResults(fResults ...*FutureResult) *FutureResult {
	all := &FutureResult{
		Done: atomic.Bool{},
		ResultChan: make(chan string , len(fResults)),
	}

	go func ()  {
		for _ , r := range fResults{
			all.ResultChan <- r.Await()
		}
		all.Done.Store(true)
	}()
		

	return all
}



func TaskDownload() string{
	time.Sleep(200 * time.Millisecond)
	return "Download"
}

func TaskUpload() string{
	time.Sleep(200 * time.Millisecond)
	return "upload"
}

func main(){
	result := Async(TaskDownload)
	result1 := Async(TaskUpload)

	// res:= result.Await()
	// res1:= result1.Await()

	com:= CombineFutureResults(result , result1)

    fmt.Println(<-com.ResultChan)
    fmt.Println(<-com.ResultChan)
	
	// fmt.Println(res , res1)

	fmt.Println("Main Done!")

}