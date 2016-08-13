// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

type StopTime struct {
	Arrival_time        Time
	Departure_time      Time
	Stop                *Stop
	Sequence            int
	Headsign            string
	Pickup_type         int8
	Drop_off_type       int8
	Shape_dist_traveled float32
	Timepoint           bool
	Has_dist            bool
}

type Time struct {
	Hour   int
	Minute int8
	Second int8
}

type StopTimes []StopTime

func (stopTimes StopTimes) Len() int {
	return len(stopTimes)
}

func (stopTimes StopTimes) Less(i, j int) bool {
	return stopTimes[i].Sequence < stopTimes[j].Sequence
}

func (stopTimes StopTimes) Swap(i, j int) {
	stopTimes[i], stopTimes[j] = stopTimes[j], stopTimes[i]
}

func (a Time) Equals(b Time) bool {
	return a.Hour == b.Hour && a.Minute == b.Minute && a.Second == b.Second
}

func (a Time) SecondsSinceMidnight() int {
	return int(a.Hour)*3600 + int(a.Minute)*60 + int(a.Second)
}

func (s StopTime) HasDistanceTraveled() bool {
	return s.Has_dist
}
