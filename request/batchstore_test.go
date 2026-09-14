/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"context"
	"encoding/binary"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
)

// TestFetchWithCanceledContext reproduces a deadlock:
// when the context passed to Fetch is canceled around the time Fetch is called,
// the ctx.Done goroutine may Signal before Fetch reaches signal.Wait(). Since a
// sync.Cond signal delivered with no waiter is lost, Wait() then blocks forever
// while holding bs.lock, which in turn deadlocks Pool.Close.
func TestFetchWithCanceledContext(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)

	for i := 0; i < 2000; i++ {
		bs := NewBatchStore(100, 100*8, func(string) {}, sugaredLogger)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // context is already canceled before Fetch is called

		done := make(chan struct{})
		go func() {
			bs.Fetch(ctx) // must return promptly; canceled ctx => empty batch
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("Fetch deadlocked on a canceled context (iteration %d)", i)
		}
	}
}

func TestBatchStore(t *testing.T) {
	max := uint32(100)
	lenByte := uint32(8)
	var removed uint32

	sugaredLogger := testutil.CreateLogger(t, 0)

	bs := NewBatchStore(max, max*lenByte, func(string) {
		atomic.AddUint32(&removed, 1)
	}, sugaredLogger)
	assert.NotNil(t, bs)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fetched := bs.Fetch(ctx)
	assert.Len(t, fetched, 0)

	requestInspector := &reqInspector{}

	workerNum := runtime.NumCPU()
	workPerWorker := 1000

	loaded := make(chan string, workerNum*workPerWorker)

	var wg sync.WaitGroup
	wg.Add(workerNum)
	var inserted uint32

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, lenByte)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				if bs.Insert(keyID, key, uint32(len(key))) {
					atomic.AddUint32(&inserted, 1)
				}
				loaded <- keyID
			}
		}(worker)
	}

	wg.Wait()
	close(loaded)

	assert.Equal(t, workerNum*workPerWorker, int(inserted))

	for i := 0; i < 10; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fetched = bs.Fetch(ctx)
		assert.Len(t, fetched, int(max))
	}

	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, lenByte)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				bs.Remove(keyID)
			}
		}(worker)
	}

	wg.Wait()

	assert.Equal(t, workerNum*workPerWorker, int(removed))
}
