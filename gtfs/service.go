// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	"time"
)

// A Service object describes exactly on what days a trip is served
type Service struct {
	Id         string
	Daymap     [7]bool
	Start_date Date
	End_date   Date
	Exceptions map[Date]int8
}

// A Date object as used in GTFS
type Date struct {
	Day   int8
	Month int8
	Year  int16
}

// IsActiveOn returns true if the service is active on a particular date
func (s *Service) IsActiveOn(d Date) bool {
	return s.GetExceptionTypeOn(d) == 1 || (s.Daymap[int(d.GetTime().Weekday())] && !(d.GetTime().Before(s.Start_date.GetTime())) && !(d.GetTime().After(s.End_date.GetTime())) && s.GetExceptionTypeOn(d) < 2)
}

// GetExceptionTypeOn returns the expection type on a particular day
func (s *Service) GetExceptionTypeOn(d Date) int8 {
	if t, ok := s.Exceptions[d]; ok {
		return t
	}

	return 0
}

// GetGtfsDateFromTime constructs a GTFS Date object from a Time object
func GetGtfsDateFromTime(t time.Time) Date {
	return Date{int8(t.Day()), int8(t.Month()), int16(t.Year())}
}

// GetOffsettedDate returns a date offsetted by a certain number of days
func (d Date) GetOffsettedDate(offset int) Date {
	return GetGtfsDateFromTime((d.GetTime().AddDate(0, 0, offset)))
}

// Equals returns true if the service is exactly the same - that is
// if it is active on exactly the same days
func (s *Service) Equals(b *Service) bool {
	startA := s.GetFirstDefinedDate()
	endA := s.GetLastDefinedDate()

	startB := b.GetFirstDefinedDate()
	endB := b.GetLastDefinedDate()

	if endA.GetTime().Before(endB.GetTime()) {
		endA = endB
	}

	if startA.GetTime().After(startB.GetTime()) {
		startA = startB
	}

	if endA.GetTime().Before(endB.GetTime()) {
		endA = endB
	}

	for d := startA; !d.GetTime().After(endA.GetTime()); d = d.GetOffsettedDate(1) {
		if s.IsActiveOn(d) != b.IsActiveOn(d) {
			return false
		}
	}

	return true
}

// GetFirstDefinedDate returns the first date something is defined
// (either positively or negatively) in this service
func (s *Service) GetFirstDefinedDate() Date {
	var first Date

	for date := range s.Exceptions {
		if first.Year == 0 || date.GetTime().Before(first.GetTime()) {
			first = date
		}
	}

	if first.Year == 0 || (s.Start_date.Year > 0 && s.Start_date.GetTime().Before(first.GetTime())) {
		first = s.Start_date
	}

	return first
}

// GetLastDefinedDate returns the last date something is defined
// (either positively or negatively) in this service
func (s *Service) GetLastDefinedDate() Date {
	var last Date

	for date := range s.Exceptions {
		if last.Year == 0 || date.GetTime().After(last.GetTime()) {
			last = date
		}
	}

	if last.Year == 0 || (s.End_date.Year > 0 && s.End_date.GetTime().After(last.GetTime())) {
		last = s.End_date
	}

	return last
}

func (d *Service) IsEmpty() bool {
	return d.Daymap[0] == false && d.Daymap[1] == false && d.Daymap[2] == false && d.Daymap[3] == false && d.Daymap[4] == false && d.Daymap[5] == false && d.Daymap[6] == false && len(d.Exceptions) == 0
}

// GetTime constructs a time object from this date, at 12:00:00 noon
func (d Date) GetTime() time.Time {
	return time.Date(int(d.Year), time.Month(d.Month), int(d.Day), 12, 0, 0, 0, time.UTC)
}
