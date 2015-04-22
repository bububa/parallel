// The parallel package provides a way of running functions
// concurrently while limiting the maximum number
// running at once.
package parallel

import (
	"fmt"
	"github.com/getsentry/raven-go"
	"log"
	"runtime"
	"sync"
	"time"
)

const SENTRY_DNS = "https://40a8b3a4b5724b73a182a45dd888583e:082932b71c7a489cb32a9813acbb33b4@sentry.xibao100.com/3"

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
	r := &Run{
		limiter: make(chan struct{}, maxPar),
		timeOut: timeOut,
	}
	return r
}

func (r *Run) SetSentry(sentry *raven.Client) {
	r.sentry = sentry
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
			f()
		} else {
			jobDone := make(chan struct{}, 1)
			go func() {
				defer func() {
					jobDone <- struct{}{}
				}()
				if r.sentry != nil {
					r.sentry.CapturePanic(func() {
						f()
					}, nil, nil)
				} else {
					defer func() {
						if recovered := recover(); recovered != nil {
							log.Println(recovered)
						}
					}()
					f()
				}
			}()
			timeoutTimer := time.NewTimer(r.timeOut * time.Second)
			defer timeoutTimer.Stop()
			for {
				select {
				case <-jobDone:
					return
				case <-timeoutTimer.C:
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
	return nil
}
