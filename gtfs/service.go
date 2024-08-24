// Copyright 2023 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import "time"

// A Service object describes exactly on what days a trip is served
type Service struct {
	ID         string
	Daymap     uint8
	StartDate  Date
	EndDate    Date
	Exceptions map[Date]bool
}

func EmptyService() *Service {
	return &Service{"", 0, Date{}, Date{}, make(map[Date]bool, 0)}
}

func (s *Service) Day(i int) bool {
	return (s.Daymap & (1 << uint8(i))) > 0
}

func (s *Service) SetDay(i int, v bool) {
	if v {
		s.Daymap |= (1 << uint8(i))
	} else {
		s.Daymap &= ^(1 << uint8(i))
	}
}

// A Date object as used in GTFS
type Date struct {
	day   uint8
	month uint8
	year  uint8
}

func NewDate(day uint8, month uint8, year uint16) Date {
	return Date{day, month, uint8(year - 1900)}
}

func (d Date) IsEmpty() bool {
	return d.day == 0 && d.month == 0 && d.year == 0
}

func (d Date) Day() uint8 {
	return d.day
}

func (d *Date) SetDay(day uint8) {
	d.day = day
}

func (d Date) Month() uint8 {
	return d.month
}

func (d Date) Year() uint16 {
	return uint16(d.year) + 1900
}

func (d *Date) SetYear(year uint16) {
	d.year = uint8(year - 1900)
}

// IsActiveOn returns true if the service is active on a particular date
func (s *Service) IsActiveOn(d Date) bool {
	exType := s.GetExceptionTypeOn(d)
	if exType == 1 {
		return true
	}
	if exType == 2 {
		return false
	}
	t := d.GetTime()
	return s.Day(int(t.Weekday())) && !(t.Before(s.StartDate.GetTime())) && !(t.After(s.EndDate.GetTime()))
}

// GetExceptionTypeOn returns the expection type on a particular day
func (s *Service) GetExceptionTypeOn(d Date) int8 {
	if t, ok := s.Exceptions[d]; ok {
		if t {
			return 1
		} else {
			return 2
		}
	}

	return 0
}

// SetExceptionTypeOn sets the expection type on a particular day
func (s *Service) SetExceptionTypeOn(d Date, t int8) {
	if t == 1 {
		s.Exceptions[d] = true
	} else if t == 2 {
		s.Exceptions[d] = false
	}
}

// GetGtfsDateFromTime constructs a GTFS Date object from a Time object
func GetGtfsDateFromTime(t time.Time) Date {
	return NewDate(uint8(t.Day()), uint8(t.Month()), uint16(t.Year()))
}

// GetOffsettedDate returns a date offsetted by a certain number of days
func (d Date) GetOffsettedDate(offset int) Date {
	if (offset == 1 || offset == -1) && d.Day() > 1 && d.Day() < 27 {
		// shortcut
		d.SetDay(uint8(int(d.Day()) + offset))
		return d
	}
	return GetGtfsDateFromTime((d.GetTime().AddDate(0, 0, offset)))
}

// Equals returns true if the service is exactly the same - that is
// if it is active on exactly the same days
func (s *Service) Equals(b *Service) bool {
	if s == b {
		// shortcut
		return true
	}

	// shortcut
	if !s.StartDate.IsEmpty() && !b.StartDate.IsEmpty() && len(s.Exceptions) == 0 && len(b.Exceptions) == 0 {
		return s.StartDate == b.StartDate && s.EndDate == b.EndDate && s.Daymap == b.Daymap
	}

	startA := s.GetFirstDefinedDate()
	endB := b.GetLastDefinedDate()

	if startA.GetTime().After(endB.GetTime()) {
		return false
	}

	startB := b.GetFirstDefinedDate()
	endA := s.GetLastDefinedDate()

	if startB.GetTime().After(endA.GetTime()) {
		return false
	}

	if endA.GetTime().Before(endB.GetTime()) {
		endA = endB
	}

	if startA.GetTime().After(startB.GetTime()) {
		startA = startB
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
		if first.IsEmpty() || date.GetTime().Before(first.GetTime()) {
			first = date
		}
	}

	if first.IsEmpty() || (!s.StartDate.IsEmpty() && s.StartDate.GetTime().Before(first.GetTime())) {
		first = s.StartDate
	}

	return first
}

// GetLastDefinedDate returns the last date something is defined
// (either positively or negatively) in this service
func (s *Service) GetLastDefinedDate() Date {
	var last Date

	for date := range s.Exceptions {
		if last.IsEmpty() || date.GetTime().After(last.GetTime()) {
			last = date
		}
	}

	if last.IsEmpty() || (!s.EndDate.IsEmpty() && s.EndDate.GetTime().After(last.GetTime())) {
		last = s.EndDate
	}

	return last
}

// GetFirstActiveDate returns the first active date of this service
func (s *Service) GetFirstActiveDate() Date {
	start := s.GetFirstDefinedDate()
	end := s.GetLastDefinedDate()
	for d := start; !d.GetTime().After(end.GetTime()); d = d.GetOffsettedDate(1) {
		if s.IsActiveOn(d) {
			return d
		}
	}

	return Date{}
}

// GetLastActiveDate returns the first active date of this service
func (s *Service) GetLastActiveDate() Date {
	start := s.GetFirstDefinedDate()
	end := s.GetLastDefinedDate()
	for d := end; !d.GetTime().Before(start.GetTime()); d = d.GetOffsettedDate(-1) {
		if s.IsActiveOn(d) {
			return d
		}
	}

	return Date{}
}

func (d *Service) IsEmpty() bool {
	return d.Daymap == 0 && len(d.Exceptions) == 0
}

// GetTime constructs a time object from this date, at 12:00:00 noon
func (d Date) GetTime() time.Time {
	return time.Date(int(d.Year()), time.Month(d.Month()), int(d.Day()), 12, 0, 0, 0, time.UTC)
}
