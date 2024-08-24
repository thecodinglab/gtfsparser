// Copyright 2023 Patrick Brosi
// Authors: info@patrickbrosi.de
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
	ArrivalTime       Time
	DepartureTime     Time
	PickupDropOff     uint8
	Stop              *Stop
	Headsign          *string
	Seq               int32
	ShapeDistTraveled float32
}

// StopTimes group multiple StopTime objects
type StopTimes []StopTime

// Time is a simple GTFS time type
type Time struct {
	Hour   int8
	Minute int8
	Second int8
}

func (st *StopTime) Sequence() int {
	if st.Seq == 0 {
		return 1
	}
	if st.Seq < 0 {
		return int(-(st.Seq + 1))
	} else {
		return int(st.Seq - 1)
	}
}

func (st *StopTime) SetSequence(seq int) {
	if st.Seq < 0 {
		st.Seq = int32(-seq - 1)
	} else {
		st.Seq = int32(seq + 1)
	}
}

func (st *StopTime) Pickup() uint8 {
	return (st.PickupDropOff & uint8(3))
}

func (st *StopTime) SetPickup(put uint8) {
	st.PickupDropOff |= put
}

func (st *StopTime) DropOff() uint8 {
	return ((st.PickupDropOff & (uint8(3) << 2)) >> 2)
}

func (st *StopTime) SetDropOff(dot uint8) {
	st.PickupDropOff |= (dot << 2)
}

func (st *StopTime) ContinuousPickup() uint8 {
	return ((st.PickupDropOff & (uint8(3) << 4)) >> 4)
}

func (st *StopTime) SetContinuousPickup(cp uint8) {
	st.PickupDropOff |= (cp << 4)
}

func (st *StopTime) ContinuousDropOff() uint8 {
	return ((st.PickupDropOff & (uint8(3) << 6)) >> 6)
}

func (st *StopTime) SetContinuousDropOff(cdo uint8) {
	st.PickupDropOff |= cdo << 6
}

func (st *StopTime) Timepoint() bool {
	return !(st.Seq < 0)
}

func (st *StopTime) SetTimepoint(tp bool) {
	if st.Seq == 0 {
		st.Seq = 1
	}
	if tp && st.Seq < 0 {
		st.Seq = -st.Seq
	} else if !tp && !(st.Seq < 0) {
		st.Seq = -st.Seq
	}
}

// Minus subtracts one time from another
func (a Time) Minus(b Time) int {
	return a.SecondsSinceMidnight() - b.SecondsSinceMidnight()
}

func (stopTimes StopTimes) Len() int {
	return len(stopTimes)
}

func (stopTimes StopTimes) Less(i, j int) bool {
	return stopTimes[i].Sequence() < stopTimes[j].Sequence()
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

	return time.Date(int(d.Year()), time.Month(d.Month()), int(d.Day()), int(a.Hour), int(a.Minute), int(a.Second), 0, loc)
}

// HasDistanceTraveled returns true if this ShapePoint has a measurement
func (s StopTime) HasDistanceTraveled() bool {
	return !math.IsNaN(float64(s.ShapeDistTraveled))
}
