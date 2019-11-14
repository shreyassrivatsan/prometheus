// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scrape

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
)

const sep = '\xff'

type exemplarVal struct {
	l labels.Labels
	t int64
	e exemplar.Exemplar
}

// exemplarStore is an in-memory implementation of the exemplars interface.
type exemplarStore struct {
	sync.RWMutex
	// TODO: need to be able to age out information.
	data map[uint64][]exemplarVal
}

// NewExemplarStore returns a new exemplar storage.
func NewExemplarStore() exemplar.Storage {
	return &exemplarStore{data: make(map[uint64][]exemplarVal)}
}

func (ex *exemplarStore) Add(l labels.Labels, t int64, e exemplar.Exemplar) error {
	hash := hashLabelsTs(l, t)

	ex.Lock()
	ex.data[hash] = append(ex.data[hash], exemplarVal{l, t, e})
	ex.Unlock()

	return nil
}

func (ex *exemplarStore) Get(l labels.Labels, t int64) (exemplar.Exemplar, bool, error) {
	hash := hashLabelsTs(l, t)

	ex.RLock()
	defer ex.RUnlock()

	val, ok := ex.data[hash]
	if !ok {
		return exemplar.Exemplar{}, false, nil
	}
	if len(val) == 1 {
		return val[0].e, true, nil
	}

	// If we find more than 1, then go over the exemplars and compare.
	for _, e := range val {
		if e.t == t && compareLabels(e.l, l) {
			return e.e, true, nil
		}
	}
	return exemplar.Exemplar{}, false, nil
}

func (ex *exemplarStore) Query(l labels.Labels, s, e int64) ([]exemplar.Exemplar, error) {
	return nil, fmt.Errorf("not implemented")
}

func (ex *exemplarStore) Close() error {
	return nil
}

// hashLabelsTs returns a hash value for the label set.
func hashLabelsTs(l labels.Labels, t int64) uint64 {
	// TODO: reuse byte array on which we are computing the hash.
	b := make([]byte, 0, 1024)

	for _, v := range l {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	b = appendTs(b, uint64(t))
	return xxhash.Sum64(b)
}

func appendTs(b []byte, t uint64) []byte {
	b = append(b, byte(t))
	b = append(b, byte(t>>8))
	b = append(b, byte(t>>16))
	b = append(b, byte(t>>24))
	b = append(b, byte(t>>32))
	b = append(b, byte(t>>40))
	b = append(b, byte(t>>48))
	b = append(b, byte(t>>56))
	return b
}

func compareLabels(l1, l2 labels.Labels) bool {
	if len(l1) != len(l2) {
		return false
	}
	l1Map := make(map[string]string, len(l1))
	for _, v := range l1 {
		l1Map[v.Name] = v.Value
	}
	for _, v := range l2 {
		if _, ok := l1Map[v.Name]; !ok {
			return false
		}
	}
	return true
}
