// The parallel package provides a way of running functions
// concurrently while limiting the maximum number
// running at once.
package parallel

import (
	"fmt"
	"github.com/bububa/raven-go/raven"
	"log"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

// Run represents a number of functions running concurrently.
type Run struct {
	limiter chan struct{}
	//done chan error
	//err chan error
	//quit bool
	sentry  *raven.Client
	timeOut time.Duration
	wg      sync.WaitGroup
}

// Errors holds any errors encountered during
// the parallel run.
type Errors []error

func (errs Errors) Error() string {
	switch len(errs) {
	case 0:
		return "no error"
	case 1:
		return errs[0].Error()
	}
	return fmt.Sprintf("%s (and %d more)", errs[0].Error(), len(errs)-1)
}

// NewRun returns a new parallel instance.  It will run up to maxPar
// functions concurrently.
func NewRun(maxPar int, timeOut time.Duration) *Run {
	sentry, _ := raven.NewClient("http://4b27200647234ab2a00c4452df8afe1c:a879047d88e1455dbf0ea055f3826c4b@sentry.xibao100.com/3")
	r := &Run{
		limiter: make(chan struct{}, maxPar),
		//done: make(chan error),
		//err: make(chan error),
		//quit: false,
		timeOut: timeOut,
		sentry:  sentry,
	}
	/*
	   go func() {
	           var errs Errors
	           for e := range r.done {
	                   errs = append(errs, e)
	           }
	           // TODO sort errors by original order of Do request?
	           if len(errs) > 0 {
	                   r.err <- errs
	           } else {
	                   r.err <- nil
	           }
	   }()
	*/
	return r
}

func (r *Run) Running() (int, int) {
	return len(r.limiter), cap(r.limiter)
}

// Do requests that r run f concurrently.  If there are already the maximum
// number of functions running concurrently, it will block until one of
// them has completed. Do may itself be called concurrently.
func (r *Run) Do(f func() error) {
	r.limiter <- struct{}{}
	r.wg.Add(1)
	go func() {
		defer func() {
			r.wg.Done()
			<-r.limiter
		}()
		if r.timeOut == 0 {
			/*if err := f(); err != nil {
			    if !r.quit {
			        r.done <- err
			    }
			}*/
			f()
		} else {
			jobDone := make(chan struct{}, 1)
			go func() {
				defer func() {
					if re := recover(); re != nil {
						log.Printf("Recover in f %v\n", re)
						debug.PrintStack()
						if r.sentry != nil {
							r.sentry.CaptureMessage(string(debug.Stack()))
						}
					}
					jobDone <- struct{}{}
				}()
				f()
				/*if err := f(); err != nil {
				    if !r.quit {
				        r.done <- err
				    }
				}*/
			}()
			for {
				select {
				case <-jobDone:
					return
				case <-time.After(r.timeOut * time.Second):
					log.Println("TimeOut: ", r.timeOut, " sec.")
					return
				}
			}
		}
		runtime.Gosched()
	}()
}

// Wait marks the parallel instance as complete and waits for all the
// functions to complete.  If any errors were encountered, it returns an
// Errors value describing all the errors in arbitrary order.
func (r *Run) Wait() error {
	r.wg.Wait()
	/*
	   if !r.quit {
	       close(r.done)
	   }
	   r.quit = true
	*/
	return nil
}
