// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	"time"
)

type Service struct {
	Id         string
	Daymap     [7]bool
	Start_date Date
	End_date   Date
	Exceptions map[Date]int8
}

type Date struct {
	Day   int8
	Month int8
	Year  int16
}

func (s *Service) IsActiveOn(d Date) bool {
	return s.GetExceptionTypeOn(d) == 1 || (s.Daymap[int(d.GetTime().Weekday())] && !(d.GetTime().Before(s.Start_date.GetTime())) && !(d.GetTime().After(s.End_date.GetTime())) && s.GetExceptionTypeOn(d) < 2)
}

func (s *Service) GetExceptionTypeOn(d Date) int8 {
	if t, ok := s.Exceptions[d]; ok {
		return t
	}

	return 0
}

func GetGtfsDateFromTime(t time.Time) Date {
	return Date{int8(t.Day()), int8(t.Month()), int16(t.Year())}
}

func (d Date) GetOffsettedDate(offset int) Date {
	return GetGtfsDateFromTime((d.GetTime().AddDate(0, 0, offset)))
}

func (a *Service) Equals(b *Service) bool {
	startA := a.GetFirstDefinedDate()
	endA := a.GetLastDefinedDate()

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
		if a.IsActiveOn(d) != b.IsActiveOn(d) {
			return false
		}
	}

	return true
}

func (service *Service) GetFirstDefinedDate() Date {
	var first Date

	for date, _ := range service.Exceptions {
		if first.Year == 0 || date.GetTime().Before(first.GetTime()) {
			first = date
		}
	}

	if first.Year == 0 || (service.Start_date.Year > 0 && service.Start_date.GetTime().Before(first.GetTime())) {
		first = service.Start_date
	}

	return first
}

func (service *Service) GetLastDefinedDate() Date {
	var last Date

	for date, _ := range service.Exceptions {
		if last.Year == 0 || date.GetTime().After(last.GetTime()) {
			last = date
		}
	}

	if last.Year == 0 || (service.End_date.Year > 0 && service.End_date.GetTime().After(last.GetTime())) {
		last = service.End_date
	}

	return last
}

func (d Date) GetTime() time.Time {
	return time.Date(int(d.Year), time.Month(d.Month), int(d.Day), 12, 0, 0, 0, time.UTC)
}
