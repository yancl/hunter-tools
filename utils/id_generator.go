// Copyright 2017, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
)

var IDGenerator *defaultIDGenerator

func init() {
	IDGenerator = &defaultIDGenerator{}
	// initialize traceID and spanID generators.
	var rngSeed int64
	for _, p := range []interface{}{
		&rngSeed, &IDGenerator.traceIDAdd, &IDGenerator.nextSpanID, &IDGenerator.spanIDInc,
	} {
		binary.Read(crand.Reader, binary.LittleEndian, p)
	}
	IDGenerator.traceIDRand = rand.New(rand.NewSource(rngSeed))
	IDGenerator.spanIDInc |= 1
}

type defaultIDGenerator struct {
	sync.Mutex
	traceIDRand *rand.Rand
	traceIDAdd  [2]uint64
	nextSpanID  uint64
	spanIDInc   uint64
}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *defaultIDGenerator) NewSpanID() [8]byte {
	var id uint64
	for id == 0 {
		id = atomic.AddUint64(&gen.nextSpanID, gen.spanIDInc)
	}
	var sid [8]byte
	binary.LittleEndian.PutUint64(sid[:], id)
	return sid
}

// NewTraceID returns a non-zero trace ID from a randomly-chosen sequence.
// mu should be held while this function is called.
func (gen *defaultIDGenerator) NewTraceID() [16]byte {
	var tid [16]byte
	// Construct the trace ID from two outputs of traceIDRand, with a constant
	// added to each half for additional entropy.
	gen.Lock()
	binary.LittleEndian.PutUint64(tid[0:8], gen.traceIDRand.Uint64()+gen.traceIDAdd[0])
	binary.LittleEndian.PutUint64(tid[8:16], gen.traceIDRand.Uint64()+gen.traceIDAdd[1])
	gen.Unlock()
	return tid
}
