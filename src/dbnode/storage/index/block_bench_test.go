// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package index

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pkg/profile"

	"github.com/m3db/m3/src/m3ninx/doc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func BenchmarkBlockWrite(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(b)
	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	bl, err := NewBlock(blockStart, testMD, testOpts)
	require.NoError(b, err)
	defer func() {
		require.NoError(b, bl.Close())
	}()

	onIndexSeries := NewMockOnIndexSeries(ctrl)
	onIndexSeries.EXPECT().OnIndexFinalize(gomock.Any()).AnyTimes()
	onIndexSeries.EXPECT().OnIndexSuccess(gomock.Any()).AnyTimes()

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})

	fieldValues := map[string][]string{
		"fruit":     []string{"apple", "banana", "orange", "watermelon"},
		"vegetable": []string{"brocolli", "carrot", "celery", "cucumber"},
		"meat":      []string{"beef", "chicken", "pork", "steak"},
		"cheese":    []string{"cheddar", "swiss", "brie", "bleu"},
	}

	for i := 0; i < 4096; i++ {
		fields := make([]doc.Field, 0, len(fieldValues))
		for key, values := range fieldValues {
			fields = append(fields, doc.Field{
				Name:  []byte(key),
				Value: []byte(values[rand.Intn(len(values))]),
			})
		}
		batch.Append(WriteBatchEntry{
			Timestamp:     now,
			OnIndexSeries: onIndexSeries,
			EnqueuedAt:    now,
		}, doc.Document{
			ID:     []byte(fmt.Sprintf("doc.%d", i)),
			Fields: fields,
		})
	}

	if strings.ToLower(os.Getenv("PROFILE_CPU")) == "true" {
		p := profile.Start(profile.CPUProfile)
		defer p.Stop()
	}

	b.ResetTimer()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// Simulate all documents being pending on consequent
		// write batch calls
		for _, entry := range batch.entries {
			entry.result.Done = false
		}

		_, err := bl.WriteBatch(batch)
		require.NoError(b, err)

		// Reset state
		bl.(*block).Lock()
		bl.(*block).foregroundSegments = nil
		bl.(*block).Unlock()
	}
	b.StopTimer()
}
