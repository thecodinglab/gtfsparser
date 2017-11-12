// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Frequency is used to describe a periodically served trip
type Frequency struct {
	Start_time   Time
	End_time     Time
	Headway_secs int
	Exact_times  bool
}
