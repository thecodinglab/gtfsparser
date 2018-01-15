// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfsparser

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser/gtfs"
	"io"
	"math"
	"os"
	opath "path"
	"sort"
)

// A ParseOptions object holds options for parsing a the feed
type ParseOptions struct {
	UseDefValueOnError bool
	DropErroneous      bool
	DryRun             bool
}

// Feed represents a single GTFS feed
type Feed struct {
	Agencies       map[string]*gtfs.Agency
	Stops          map[string]*gtfs.Stop
	Routes         map[string]*gtfs.Route
	Trips          map[string]*gtfs.Trip
	Services       map[string]*gtfs.Service
	FareAttributes map[string]*gtfs.FareAttribute
	Shapes         map[string]*gtfs.Shape
	Transfers      []*gtfs.Transfer
	FeedInfos      []*gtfs.FeedInfo

	zipFileCloser *zip.ReadCloser
	curFileHandle *os.File

	opts ParseOptions
}

// NewFeed creates a new, empty feed
func NewFeed() *Feed {
	g := Feed{
		Agencies:       make(map[string]*gtfs.Agency),
		Stops:          make(map[string]*gtfs.Stop),
		Routes:         make(map[string]*gtfs.Route),
		Trips:          make(map[string]*gtfs.Trip),
		Services:       make(map[string]*gtfs.Service),
		FareAttributes: make(map[string]*gtfs.FareAttribute),
		Shapes:         make(map[string]*gtfs.Shape),
		Transfers:      make([]*gtfs.Transfer, 0),
		FeedInfos:      make([]*gtfs.FeedInfo, 0),
		opts:           ParseOptions{false, false, false},
	}
	return &g
}

// SetParseOpts sets the ParseOptions for this feed
func (feed *Feed) SetParseOpts(opts ParseOptions) {
	feed.opts = opts
}

// Parse the GTFS data in the specified folder into the feed
func (feed *Feed) Parse(path string) error {
	var e error

	e = feed.parseAgencies(path)
	if e == nil {
		e = feed.parseFeedInfos(path)
	}
	if e == nil {
		e = feed.parseStops(path)
	}
	if e == nil {
		e = feed.parseShapes(path)
	}

	if e == nil {
		// sort points in shapes
		for _, shape := range feed.Shapes {
			sort.Sort(shape.Points)
			e = feed.checkShapeMeasure(shape, &feed.opts)
			if e != nil {
				break
			}
		}
		if feed.opts.DryRun {
			// clear space
			for id := range feed.Shapes {
				feed.Shapes[id] = nil
			}
		}
	}

	if e == nil {
		e = feed.parseRoutes(path)
	}
	if e == nil {
		e = feed.parseCalendar(path)
	}
	if e == nil {
		e = feed.parseCalendarDates(path)
	}
	if e == nil {
		e = feed.parseTrips(path)
	}
	if e == nil {
		e = feed.parseStopTimes(path)
	}

	if e == nil {
		// sort stoptimes in trips
		for _, trip := range feed.Trips {
			sort.Sort(trip.StopTimes)
			e = feed.checkStopTimeMeasure(trip, &feed.opts)
			if e != nil {
				break
			}

			if feed.opts.DryRun {
				feed.Trips[trip.Id] = nil
			}
		}
	}

	if e == nil {
		e = feed.parseFareAttributes(path)
	}
	if e == nil {
		e = feed.parseFareAttributeRules(path)
	}
	if e == nil {
		e = feed.parseFrequencies(path)
	}
	if e == nil {
		e = feed.parseTransfers(path)
	}

	// close open readers
	if feed.zipFileCloser != nil {
		feed.zipFileCloser.Close()
	}

	if feed.curFileHandle != nil {
		feed.curFileHandle.Close()
	}

	return e
}

func (feed *Feed) getFile(path string, name string) (io.Reader, error) {
	fileInfo, err := os.Stat(path)

	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		if feed.curFileHandle != nil {
			// close previous handle
			feed.curFileHandle.Close()
		}

		return os.Open(opath.Join(path, name))
	}

	var e error
	if feed.zipFileCloser == nil {
		// reuse existing opened zip file
		feed.zipFileCloser, e = zip.OpenReader(path)
	}

	if e != nil {
		return nil, e
	}

	for _, f := range feed.zipFileCloser.File {
		if f.Name == name {
			return f.Open()
		}
	}

	return nil, errors.New("Not found")
}

func (feed *Feed) parseAgencies(path string) (err error) {
	file, e := feed.getFile(path, "agency.txt")

	if e != nil {
		return errors.New("Could not open required file agency.txt")
	}

	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"agency.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		agency, e := createAgency(record, &feed.opts)
		if e == nil {
			if _, ok := feed.Agencies[agency.Id]; ok {
				e = errors.New("ID collision, agency_id '" + agency.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
		feed.Agencies[agency.Id] = agency
	}

	return e
}

func (feed *Feed) parseStops(path string) (err error) {
	file, e := feed.getFile(path, "stops.txt")

	if e != nil {
		return errors.New("Could not open required file stops.txt")
	}

	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"stops.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	parentStopIds := make(map[string]string, 0)
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		stop, e := createStop(record, &feed.opts)
		if e == nil {
			if _, ok := feed.Stops[stop.Id]; ok {
				e = errors.New("ID collision, stop_id '" + stop.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
		if v, in := record["parent_station"]; in && len(v) > 0 {
			parentStopIds[stop.Id] = v
		}
		feed.Stops[stop.Id] = stop
	}

	// write the parent stop ids
	for id, pid := range parentStopIds {
		pstop, ok := feed.Stops[pid]
		if !ok {
			if feed.opts.UseDefValueOnError {
				// continue, the default value "nil" has already be written above
				continue
			} else if feed.opts.DropErroneous {
				// delete the erroneous entry
				delete(feed.Stops, id)
			} else {
				panic(errors.New("(for stop id " + id + ") No station with id " + pid + " found, cannot use as parent station here."))
			}
		}
		feed.Stops[id].Parent_station = pstop
	}

	return e
}

func (feed *Feed) parseRoutes(path string) (err error) {
	file, e := feed.getFile(path, "routes.txt")

	if e != nil {
		return errors.New("Could not open required file routes.txt")
	}

	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"routes.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		route, e := createRoute(record, feed.Agencies, &feed.opts)
		if e == nil {
			if _, ok := feed.Routes[route.Id]; ok {
				e = errors.New("ID collision, route_id '" + route.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
		if feed.opts.DryRun {
			feed.Routes[route.Id] = nil
		} else {
			feed.Routes[route.Id] = route
		}
	}
	return e
}

func (feed *Feed) parseCalendar(path string) (err error) {
	file, e := feed.getFile(path, "calendar.txt")

	if e != nil {
		return nil
	}

	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"calendar.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		service, e := createServiceFromCalendar(record, feed.Services, &feed.opts)

		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}

		// if service was parsed in-place, nil was returned
		if service != nil {
			if feed.opts.DryRun {
				feed.Services[service.Id] = nil
			} else {
				feed.Services[service.Id] = service
			}
		}
	}

	return e
}

func (feed *Feed) parseCalendarDates(path string) (err error) {
	file, e := feed.getFile(path, "calendar_dates.txt")

	if e != nil {
		return nil
	}

	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"calendar_dates.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		service, e := createServiceFromCalendarDates(record, feed.Services)

		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}

		// if service was parsed in-place, nil was returned
		if service != nil {
			if feed.opts.DryRun {
				feed.Services[service.Id] = nil
			} else {
				feed.Services[service.Id] = service
			}
		}
	}

	return e
}

func (feed *Feed) parseTrips(path string) (err error) {
	file, e := feed.getFile(path, "trips.txt")

	if e != nil {
		return errors.New("Could not open required file trips.txt")
	}

	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"trips.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		trip, e := createTrip(record, feed.Routes, feed.Services, feed.Shapes, &feed.opts)
		if e == nil {
			if _, ok := feed.Trips[trip.Id]; ok {
				e = errors.New("ID collision, trip_id '" + trip.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
		feed.Trips[trip.Id] = trip
	}

	return e
}

func (feed *Feed) parseShapes(path string) (err error) {
	file, e := feed.getFile(path, "shapes.txt")

	if e != nil {
		return nil
	}

	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"shapes.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createShapePoint(record, feed.Shapes, &feed.opts)
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
	}

	return e
}

func (feed *Feed) parseStopTimes(path string) (err error) {
	file, e := feed.getFile(path, "stop_times.txt")

	if e != nil {
		return errors.New("Could not open required file stop_times.txt")
	}
	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"stop_times.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createStopTime(record, feed.Stops, feed.Trips, &feed.opts)

		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
	}

	return e
}

func (feed *Feed) parseFrequencies(path string) (err error) {
	file, e := feed.getFile(path, "frequencies.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"frequencies.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createFrequency(record, feed.Trips, &feed.opts)
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
	}

	return e
}

func (feed *Feed) parseFareAttributes(path string) (err error) {
	file, e := feed.getFile(path, "fare_attributes.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"fare_attributes.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		fa, e := createFareAttribute(record, &feed.opts)
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
		feed.FareAttributes[fa.Id] = fa
	}

	return e
}

func (feed *Feed) parseFareAttributeRules(path string) (err error) {
	file, e := feed.getFile(path, "fare_rules.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"fare_rules.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createFareRule(record, feed.FareAttributes, feed.Routes)
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
	}

	return e
}

func (feed *Feed) parseTransfers(path string) (err error) {
	file, e := feed.getFile(path, "transfers.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"transfers.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		t, e := createTransfer(record, feed.Stops, &feed.opts)
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
		if !feed.opts.DryRun {
			feed.Transfers = append(feed.Transfers, t)
		}
	}

	return e
}

func (feed *Feed) parseFeedInfos(path string) (err error) {
	file, e := feed.getFile(path, "feed_info.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"feed_info.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		fi, e := createFeedInfo(record, &feed.opts)
		if e != nil {
			if feed.opts.DropErroneous {
				continue
			} else {
				panic(e)
			}
		}
		if !feed.opts.DryRun {
			feed.FeedInfos = append(feed.FeedInfos, fi)
		}
	}

	return e
}

func (feed *Feed) checkShapeMeasure(shape *gtfs.Shape, opt *ParseOptions) error {
	max := float32(math.Inf(-1))
	deleted := 0
	for j := 1; j < len(shape.Points)+deleted; j++ {
		i := j - deleted
		if shape.Points[i-1].HasDistanceTraveled() && shape.Points[i-1].Dist_traveled > max {
			max = shape.Points[i-1].Dist_traveled
		}

		if shape.Points[i].HasDistanceTraveled() && max > shape.Points[i].Dist_traveled {
			if opt.UseDefValueOnError {
				shape.Points[i].Dist_traveled = 0
				shape.Points[i].Has_dist = false
			} else if opt.DropErroneous {
				shape.Points = shape.Points[:i+copy(shape.Points[i:], shape.Points[i+1:])]
				deleted++
			} else {
				return fmt.Errorf("In shape '%s' for point with seq=%d shape_dist_traveled does not increase along with stop_sequence (%f > %f)", shape.Id, shape.Points[i].Sequence, max, shape.Points[i].Dist_traveled)
			}
		}
	}
	return nil
}

func (feed *Feed) checkStopTimeMeasure(trip *gtfs.Trip, opt *ParseOptions) error {
	max := float32(math.Inf(-1))
	deleted := 0
	for j := 1; j < len(trip.StopTimes)+deleted; j++ {
		i := j - deleted

		if !trip.StopTimes[i-1].Departure_time.Empty() && !trip.StopTimes[i].Arrival_time.Empty() && trip.StopTimes[i-1].Departure_time.SecondsSinceMidnight() > trip.StopTimes[i].Arrival_time.SecondsSinceMidnight() {
			if opt.DropErroneous {
				trip.StopTimes = trip.StopTimes[:i+copy(trip.StopTimes[i:], trip.StopTimes[i+1:])]
				deleted++
			} else {
				return fmt.Errorf("In trip '%s' for stoptime with seq=%d the arrival time is before the departure in the previous station", trip.Id, trip.StopTimes[i].Sequence)
			}
		}

		if trip.StopTimes[i-1].HasDistanceTraveled() && trip.StopTimes[i-1].Shape_dist_traveled > max {
			max = trip.StopTimes[i-1].Shape_dist_traveled
		}

		if trip.StopTimes[i].HasDistanceTraveled() && max > trip.StopTimes[i].Shape_dist_traveled {
			if opt.UseDefValueOnError {
				trip.StopTimes[i].Shape_dist_traveled = 0
				trip.StopTimes[i].Has_dist = false
			} else if opt.DropErroneous {
				trip.StopTimes = trip.StopTimes[:i+copy(trip.StopTimes[i:], trip.StopTimes[i+1:])]
				deleted++
			} else {
				return fmt.Errorf("In trip '%s' for stoptime with seq=%d shape_dist_traveled does not increase along with stop_sequence (%f > %f)", trip.Id, trip.StopTimes[i].Sequence, max, trip.StopTimes[i].Shape_dist_traveled)
			}
		}
	}
	return nil
}
