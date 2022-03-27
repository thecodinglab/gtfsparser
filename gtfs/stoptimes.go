// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	"math"
	"time"
)

// A StopTime is a single stop with times on a trip
type StopTime struct {
	Arrival_time        Time
	Departure_time      Time
	Stop                *Stop
	Sequence            int
	Headsign            string
	Pickup_type         int8
	Drop_off_type       int8
	Continuous_pickup   int8
	Continuous_drop_off int8
	Shape_dist_traveled float32
	Timepoint           bool
}

// Time is a simple GTFS time type
type Time struct {
	Hour   int8
	Minute int8
	Second int8
}

// StopTimes group multiple StopTime objects
type StopTimes []StopTime

// Minus subtracts one time from another
func (a Time) Minus(b Time) int {
	return a.SecondsSinceMidnight() - b.SecondsSinceMidnight()
}

func (stopTimes StopTimes) Len() int {
	return len(stopTimes)
}

func (stopTimes StopTimes) Less(i, j int) bool {
	return stopTimes[i].Sequence < stopTimes[j].Sequence
}

func (stopTimes StopTimes) Swap(i, j int) {
	stopTimes[i], stopTimes[j] = stopTimes[j], stopTimes[i]
}

// Empty returns true if the Time is 'empty' - null
func (a Time) Empty() bool {
	return a.Hour == -1 && a.Minute == -1 && a.Second == -1
}

// Equals returns true if two Time objects are exactly the same
func (a Time) Equals(b Time) bool {
	return a.Hour == b.Hour && a.Minute == b.Minute && a.Second == b.Second
}

// SecondsSinceMidnight returns the number of seconds since midnight
func (a Time) SecondsSinceMidnight() int {
	return int(a.Hour)*3600 + int(a.Minute)*60 + int(a.Second)
}

// GetLocationTime returns the time.Time of the gtfs time on a certain
// date, for a certain agency (which itself holds a timezone)
func (a Time) GetLocationTime(d Date, agency *Agency) time.Time {
	loc := agency.Timezone.GetLocation()
	if loc == nil {
		panic("Don't know timezone " + agency.Timezone.GetTzString())
	}

	return time.Date(int(d.Year), time.Month(d.Month), int(d.Day), int(a.Hour), int(a.Minute), int(a.Second), 0, loc)
}

// HasDistanceTraveled returns true if this ShapePoint has a measurement
func (s StopTime) HasDistanceTraveled() bool {
	return !math.IsNaN(float64(s.Shape_dist_traveled))
}
