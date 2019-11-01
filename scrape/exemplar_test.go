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
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/util/testutil"
)

func TestExemplarStore(t *testing.T) {
	ts := timestamp.FromTime(time.Now())
	cases := []struct {
		name      string
		addLabels int
		addTs     int64
		getTs     int64
		found     bool
	}{
		{
			"success 0",
			0,
			ts,
			ts,
			true,
		},
		{
			"success 1",
			1,
			ts,
			ts,
			true,
		},
		{
			"success 2",
			2,
			ts,
			ts,
			true,
		},
		{
			"success 3",
			3,
			ts,
			ts,
			true,
		},
		{
			"fail",
			2,
			ts,
			ts + 1,
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := exemplar.Exemplar{Labels: labels.FromStrings("traceID", "123bca45dce")}
			store := NewExemplarStore()
			store.Add(getLabels(c.addLabels), c.addTs, e)
			res, found, _ := store.Get(getLabels(c.addLabels), c.getTs)
			testutil.Equals(t, c.found, found)
			if found {
				testutil.Equals(t, e, res)
			}
		})
	}
}

func TestExemplarMultiple(t *testing.T) {
	ts := timestamp.FromTime(time.Now())
	store := NewExemplarStore()

	e1 := exemplar.Exemplar{Labels: labels.FromStrings("traceID", "123bca45dce")}
	store.Add(getLabels(2), ts, e1)

	e2 := exemplar.Exemplar{Labels: labels.FromStrings("traceID", "223bca45dce")}
	store.Add(getLabels(2), ts+1, e2)

	e3 := exemplar.Exemplar{Labels: labels.FromStrings("traceID", "323bca45dce")}
	store.Add(getLabels(2), ts-1, e3)

	res, found, _ := store.Get(getLabels(2), ts)
	testutil.Equals(t, true, found)
	testutil.Equals(t, e1, res)

	res, found, _ = store.Get(getLabels(2), ts-1)
	testutil.Equals(t, true, found)
	testutil.Equals(t, e3, res)

	res, found, _ = store.Get(getLabels(2), ts+1)
	testutil.Equals(t, true, found)
	testutil.Equals(t, e2, res)
}

func getLabels(num int) labels.Labels {
	ls := make([]labels.Label, 0, num)
	for i := 0; i < num; i++ {
		ls = append(ls, labels.Label{
			Name:  fmt.Sprintf("name%v", i),
			Value: fmt.Sprintf("value%v", i),
		})
	}
	return ls
}
