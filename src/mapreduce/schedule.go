package mapreduce

import "fmt"
import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var wg sync.WaitGroup
	wg.Add(ntasks)
	jobs := make(chan int, ntasks)

	for i := 0; i < ntasks; i++ {
		jobs <- i
	}

	go func() {
		wg.Wait()
		close(jobs)
	}()

	for i := range jobs {
		f := ""
		if phase == mapPhase {
			f = mr.files[i]
		}
		w := <-mr.registerChannel
		go func(n int, file string, worker string) {
			args := DoTaskArgs{
				JobName:       mr.jobName,
				Phase:         phase,
				File:          file,
				TaskNumber:    n,
				NumOtherPhase: nios,
			}
			var reply struct{}
			if call(worker, "Worker.DoTask", args, &reply) {
				wg.Done()
			} else {
				jobs <- n
			}

			mr.registerChannel <- worker
		}(i, f, w)
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
