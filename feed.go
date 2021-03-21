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

// Holds the original column ordering
type ColOrders struct {
	Agencies           []string
	Stops              []string
	Routes             []string
	Trips              []string
	StopTimes          []string
	Frequencies        []string
	Calendar           []string
	CalendarDates      []string
	FareAttributes     []string
	FareAttributeRules []string
	Shapes             []string
	Levels             []string
	Pathways           []string
	Transfers          []string
	FeedInfos          []string
	Attributions       []string
}

// A ParseOptions object holds options for parsing a the feed
type ParseOptions struct {
	UseDefValueOnError   bool
	DropErroneous        bool
	DryRun               bool
	CheckNullCoordinates bool
	EmptyStringRepl      string
	ZipFix               bool
	ShowWarnings         bool
	PolygonFilter        [][][]float64
}

type ErrStats struct {
	DroppedAgencies           int
	DroppedStops              int
	DroppedRoutes             int
	DroppedTrips              int
	DroppedStopTimes          int
	DroppedFrequencies        int
	DroppedServices           int
	DroppedFareAttributes     int
	DroppedFareAttributeRules int
	DroppedAttributions       int
	DroppedShapes             int
	DroppedLevels             int
	DroppedPathways           int
	DroppedTransfers          int
	DroppedFeedInfos          int
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
	Levels         map[string]*gtfs.Level
	Pathways       map[string]*gtfs.Pathway
	Transfers      []*gtfs.Transfer
	FeedInfos      []*gtfs.FeedInfo

	// this only holds feed-wide attributions
	Attributions []*gtfs.Attribution

	ErrorStats   ErrStats
	NumShpPoints int

	ColOrders ColOrders

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
		Levels:         make(map[string]*gtfs.Level),
		Pathways:       make(map[string]*gtfs.Pathway),
		Transfers:      make([]*gtfs.Transfer, 0),
		FeedInfos:      make([]*gtfs.FeedInfo, 0),
		ErrorStats:     ErrStats{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		NumShpPoints:   0,
		opts:           ParseOptions{false, false, false, false, "", false, false, make([][][]float64, 0)},
	}
	return &g
}

// SetParseOpts sets the ParseOptions for this feed
func (feed *Feed) SetParseOpts(opts ParseOptions) {
	feed.opts = opts
}

// Parse the GTFS data in the specified folder into the feed
func (feed *Feed) Parse(path string) error {
	return feed.PrefixParse(path, "")
}

// Parse the GTFS data in the specified folder into the feed, use
// and id prefix
func (feed *Feed) PrefixParse(path string, prefix string) error {
	var e error

	// holds stops that are dropped because of geometric filtering.
	// if these are referenced later, we quietly ignore the error like
	// with -De

	geofilteredStops := make(map[string]struct{}, 0)

	e = feed.parseAgencies(path, prefix)
	if e == nil {
		e = feed.parseFeedInfos(path)
	}
	if e == nil {
		e = feed.parseLevels(path, prefix)
	}
	if e == nil {
		e = feed.parseStops(path, prefix, geofilteredStops)
	}
	if e == nil {
		e = feed.parseShapes(path, prefix)
	}
	if e == nil {
		e = feed.parseRoutes(path, prefix)
	}
	if e == nil {
		e = feed.parseCalendar(path, prefix)
	}
	if e == nil {
		e = feed.parseCalendarDates(path, prefix)
	}
	if e == nil {
		e = feed.parseTrips(path, prefix)
	}
	if e == nil {
		e = feed.parseStopTimes(path, prefix, geofilteredStops)
	}
	if e == nil {
		e = feed.parseFareAttributes(path, prefix)
	}
	if e == nil {
		e = feed.parseFareAttributeRules(path, prefix)
	}
	if e == nil {
		e = feed.parseFrequencies(path, prefix)
	}
	if e == nil {
		e = feed.parseTransfers(path, prefix, geofilteredStops)
	}
	if e == nil {
		e = feed.parsePathways(path, prefix, geofilteredStops)
	}
	if e == nil {
		e = feed.parseAttributions(path, prefix)
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

	// check for any directory that is a ZIP file
	zipDir := feed.getGTFSDir(feed.zipFileCloser)

	if !feed.opts.ZipFix {
		zipDir = ""
	}

	for _, f := range feed.zipFileCloser.File {
		d, n := opath.Split(f.Name)
		if d == zipDir && n == name {
			return f.Open()
		}
	}

	return nil, errors.New("Not found")
}

func (feed *Feed) parseAgencies(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "agency.txt")

	if e != nil {
		return errors.New("Could not open required file agency.txt")
	}

	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"agency.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		agency, e := createAgency(record, feed, prefix)
		if e == nil {
			if _, ok := feed.Agencies[agency.Id]; ok {
				e = errors.New("ID collision, agency_id '" + agency.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedAgencies++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
		feed.Agencies[agency.Id] = agency
	}

	feed.ColOrders.Agencies = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseStops(path string, prefix string, geofiltered map[string]struct{}) (err error) {
	file, e := feed.getFile(path, "stops.txt")

	if e != nil {
		return errors.New("Could not open required file stops.txt")
	}

	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"stops.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	parentStopIds := make(map[string]string, 0)
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		stop, parentId, e := createStop(record, feed, prefix)
		if e == nil {
			if _, ok := feed.Stops[stop.Id]; ok {
				e = errors.New("ID collision, stop_id '" + stop.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedStops++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}

		// check if any defined PolygonFilter contains the stop
		contains := true
		for _, poly := range feed.opts.PolygonFilter {
			contains = false
			if len(poly) > 0 {
				if polyContains(float64(stop.Lon), float64(stop.Lat), poly) {
					contains = true
					break
				}
			}
		}

		if !contains {
			geofiltered[stop.Id] = struct{}{}
			continue
		}

		if len(parentId) > len(prefix) {
			parentStopIds[stop.Id] = parentId
		}

		feed.Stops[stop.Id] = stop
	}

	feed.ColOrders.Stops = append([]string(nil), reader.header...)

	// write the parent stop ids
	for id, pid := range parentStopIds {
		pstop, ok := feed.Stops[pid]
		if !ok {
			locErr := errors.New("(for stop id " + id + ") No station with id " + pid + " found, cannot use as parent station here.")
			_, wasFiltered := geofiltered[pid]
			if feed.opts.UseDefValueOnError {
				// continue, the default value "nil" has already be written above
				feed.warn(locErr)
				continue
			} else if wasFiltered {
				// continue, the default value "nil" has already be written above
				continue
			} else if feed.opts.DropErroneous {
				// delete the erroneous entry
				delete(feed.Stops, id)
				feed.ErrorStats.DroppedStops++
				feed.warn(locErr)
				continue
			} else {
				return locErr
			}
		}

		if (feed.Stops[id].Location_type == 0 || feed.Stops[id].Location_type == 2 || feed.Stops[id].Location_type == 3) && pstop.Location_type != 1 {
			locErr := fmt.Errorf("(for stop id %s) Station with id %s has location_type=%d, cannot use as parent station here for stop with location_type=%d (must be 1).", id, pid, pstop.Location_type, feed.Stops[id].Location_type)
			if feed.opts.UseDefValueOnError && !(feed.Stops[id].Location_type == 2 || feed.Stops[id].Location_type == 3) {
				// continue, the default value "nil" has already be written above
				feed.warn(locErr)
				continue
			} else if feed.opts.DropErroneous {
				// delete the erroneous entry
				delete(feed.Stops, id)
				feed.ErrorStats.DroppedStops++
				feed.warn(locErr)
				continue
			} else {
				return (locErr)
			}
		}

		if feed.Stops[id].Location_type == 4 && pstop.Location_type != 0 {
			locErr := fmt.Errorf("(for stop id %s) Station with id %s has location_type=%d, cannot use as parent station here for stop with location_type=4 (boarding area), which expects a parent station with location_type=0 (stop/platform).", id, pid, pstop.Location_type)
			if feed.opts.DropErroneous {
				// delete the erroneous entry
				delete(feed.Stops, id)
				feed.ErrorStats.DroppedStops++
				feed.warn(locErr)
				continue
			} else {
				panic(locErr)
			}
		}

		feed.Stops[id].Parent_station = pstop
	}

	return e
}

func (feed *Feed) parseRoutes(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "routes.txt")

	if e != nil {
		return errors.New("Could not open required file routes.txt")
	}

	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"routes.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		route, e := createRoute(record, feed, prefix)
		if e == nil {
			if _, ok := feed.Routes[route.Id]; ok {
				e = errors.New("ID collision, route_id '" + route.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedRoutes++
				feed.warn(e)
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

	feed.ColOrders.Routes = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseCalendar(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "calendar.txt")

	if e != nil {
		return nil
	}

	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"calendar.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		service, e := createServiceFromCalendar(record, feed, prefix)

		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedServices++
				feed.warn(e)
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

	feed.ColOrders.Calendar = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseCalendarDates(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "calendar_dates.txt")

	if e != nil {
		return nil
	}

	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"calendar_dates.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		service, e := createServiceFromCalendarDates(record, feed, prefix)

		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedServices++
				feed.warn(e)
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

	feed.ColOrders.CalendarDates = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseTrips(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "trips.txt")

	if e != nil {
		return errors.New("Could not open required file trips.txt")
	}

	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"trips.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		trip, e := createTrip(record, feed, prefix)
		if e == nil {
			if _, ok := feed.Trips[trip.Id]; ok {
				e = errors.New("ID collision, trip_id '" + trip.Id + "' already used.")
			}
		}
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedTrips++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
		feed.Trips[trip.Id] = trip
	}

	feed.ColOrders.Trips = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseShapes(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "shapes.txt")

	if e != nil {
		return nil
	}

	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"shapes.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createShapePoint(record, feed, prefix)
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedShapes++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
	}

	feed.ColOrders.Shapes = append([]string(nil), reader.header...)

	if e == nil {
		// sort points in shapes
		for _, shape := range feed.Shapes {
			sort.Sort(shape.Points)
			e = feed.checkShapeMeasure(shape, &feed.opts)
			feed.NumShpPoints += len(shape.Points)
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

	return e
}

func (feed *Feed) parseStopTimes(path string, prefix string, geofiltered map[string]struct{}) (err error) {
	file, e := feed.getFile(path, "stop_times.txt")

	if e != nil {
		return errors.New("Could not open required file stop_times.txt")
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"stop_times.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createStopTime(record, feed, prefix)

		if e != nil {
			stopNotFoundErr, stopNotFound := e.(*StopNotFoundErr)
			wasFiltered := false
			if stopNotFound {
				_, wasFiltered = geofiltered[stopNotFoundErr.StopId()]
			}

			if wasFiltered {
				continue
			} else if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedStopTimes++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
	}

	feed.ColOrders.StopTimes = append([]string(nil), reader.header...)

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

	return e
}

func (feed *Feed) parseFrequencies(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "frequencies.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"frequencies.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createFrequency(record, feed, prefix)
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedFrequencies++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
	}

	feed.ColOrders.Frequencies = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseFareAttributes(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "fare_attributes.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"fare_attributes.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		fa, e := createFareAttribute(record, feed, prefix)
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedFareAttributes++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
		feed.FareAttributes[fa.Id] = fa
	}

	feed.ColOrders.FareAttributes = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseFareAttributeRules(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "fare_rules.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"fare_rules.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		e := createFareRule(record, feed, prefix)
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedFareAttributeRules++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
	}

	feed.ColOrders.FareAttributeRules = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseTransfers(path string, prefix string, geofiltered map[string]struct{}) (err error) {
	file, e := feed.getFile(path, "transfers.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"transfers.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		t, e := createTransfer(record, feed, prefix)
		if e != nil {
			stopNotFoundErr, stopNotFound := e.(*StopNotFoundErr)
			wasFiltered := false
			if stopNotFound {
				_, wasFiltered = geofiltered[stopNotFoundErr.StopId()]
			}

			if wasFiltered {
				continue
			} else if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedTransfers++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
		if !feed.opts.DryRun {
			feed.Transfers = append(feed.Transfers, t)
		}
	}

	feed.ColOrders.Transfers = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parsePathways(path string, prefix string, geofiltered map[string]struct{}) (err error) {
	file, e := feed.getFile(path, "pathways.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"pathways.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		pw, e := createPathway(record, feed, prefix)
		if e == nil {
			if _, ok := feed.Pathways[pw.Id]; ok {
				e = errors.New("ID collision, pathway_id '" + pw.Id + "' already used.")
			}
		}
		if e != nil {
			stopNotFoundErr, stopNotFound := e.(*StopNotFoundErr)
			wasFiltered := false
			if stopNotFound {
				_, wasFiltered = geofiltered[stopNotFoundErr.StopId()]
			}

			if wasFiltered {
				continue
			} else if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedPathways++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
		feed.Pathways[pw.Id] = pw
	}

	feed.ColOrders.Pathways = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseAttributions(path string, prefix string) (err error) {
	file, e := feed.getFile(path, "attributions.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"attributions.txt", reader.Curline, r.(error).Error()}
		}
	}()

	ids := make(map[string]bool)

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		attr, ag, route, trip, e := createAttribution(record, feed, prefix)
		if e == nil {
			if _, ok := ids[attr.Id]; ok {
				e = errors.New("ID collision, attribution_id '" + attr.Id + "' already used.")
			}
			ids[attr.Id] = true
		}
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedAttributions++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}

		if ag != nil {
			ag.Attributions = append(ag.Attributions, attr)
		} else if route != nil {
			route.Attributions = append(route.Attributions, attr)
		} else if trip != nil {
			trip.Attributions = append(trip.Attributions, attr)
		} else {
			// if the attribution is not for a specific agency, route or trip,
			// add it to feed-wide
			feed.Attributions = append(feed.Attributions, attr)
		}
	}

	feed.ColOrders.Attributions = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseLevels(path string, idprefix string) (err error) {
	file, e := feed.getFile(path, "levels.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"levels.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		lvl, e := createLevel(record, feed, idprefix)
		if e == nil {
			if _, ok := feed.Levels[lvl.Id]; ok {
				e = errors.New("ID collision, level_id '" + lvl.Id + "' already used.")
			}
		}

		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedLevels++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
		feed.Levels[lvl.Id] = lvl
	}

	feed.ColOrders.Levels = append([]string(nil), reader.header...)

	return e
}

func (feed *Feed) parseFeedInfos(path string) (err error) {
	file, e := feed.getFile(path, "feed_info.txt")

	if e != nil {
		return nil
	}
	reader := NewCsvParser(file, feed.opts.DropErroneous)

	defer func() {
		if r := recover(); r != nil {
			err = ParseError{"feed_info.txt", reader.Curline, r.(error).Error()}
		}
	}()

	var record map[string]string
	for record = reader.ParseRecord(); record != nil; record = reader.ParseRecord() {
		fi, e := createFeedInfo(record, feed)
		if e != nil {
			if feed.opts.DropErroneous {
				feed.ErrorStats.DroppedFeedInfos++
				feed.warn(e)
				continue
			} else {
				panic(e)
			}
		}
		if !feed.opts.DryRun {
			feed.FeedInfos = append(feed.FeedInfos, fi)
		}
	}

	feed.ColOrders.FeedInfos = append([]string(nil), reader.header...)

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
			e := fmt.Errorf("In shape '%s' for point with seq=%d shape_dist_traveled does not increase along with stop_sequence (%f > %f)", shape.Id, shape.Points[i].Sequence, max, shape.Points[i].Dist_traveled)
			if opt.UseDefValueOnError {
				shape.Points[i].Dist_traveled = 0
				shape.Points[i].Has_dist = false
				feed.warn(e)
			} else if opt.DropErroneous {
				feed.ErrorStats.DroppedShapes++
				feed.warn(e)
				shape.Points = shape.Points[:i+copy(shape.Points[i:], shape.Points[i+1:])]
				deleted++
			} else {
				return e
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
			e := fmt.Errorf("In trip '%s' for stoptime with seq=%d the arrival time is before the departure in the previous station", trip.Id, trip.StopTimes[i].Sequence)
			if opt.DropErroneous {
				feed.ErrorStats.DroppedStopTimes++
				trip.StopTimes = trip.StopTimes[:i+copy(trip.StopTimes[i:], trip.StopTimes[i+1:])]
				feed.warn(e)
				deleted++
				continue
			} else {
				return e
			}
		}

		if trip.StopTimes[i-1].HasDistanceTraveled() && trip.StopTimes[i-1].Shape_dist_traveled > max {
			max = trip.StopTimes[i-1].Shape_dist_traveled
		}

		if trip.StopTimes[i].HasDistanceTraveled() && max > trip.StopTimes[i].Shape_dist_traveled {
			e := fmt.Errorf("In trip '%s' for stoptime with seq=%d shape_dist_traveled does not increase along with stop_sequence (%f > %f)", trip.Id, trip.StopTimes[i].Sequence, max, trip.StopTimes[i].Shape_dist_traveled)
			if opt.UseDefValueOnError {
				trip.StopTimes[i].Shape_dist_traveled = 0
				trip.StopTimes[i].Has_dist = false
				feed.warn(e)
			} else if opt.DropErroneous {
				trip.StopTimes = trip.StopTimes[:i+copy(trip.StopTimes[i:], trip.StopTimes[i+1:])]
				feed.ErrorStats.DroppedStopTimes++
				feed.warn(e)
				deleted++
				continue
			} else {
				return e
			}
		}
	}
	return nil
}

func polyContains(x float64, y float64, poly [][]float64) bool {
	// see https://de.wikipedia.org/wiki/Punkt-in-Polygon-Test_nach_Jordan
	c := int8(-1)

	for i := 1; i < len(poly); i++ {
		c *= polyContCheck(x, y, poly[i-1][0], poly[i-1][1], poly[i][0], poly[i][1])
		if c == 0 {
			return true
		}
	}

	c *= polyContCheck(x, y, poly[len(poly)-1][0], poly[len(poly)-1][1], poly[0][0], poly[0][1])

	return c >= 0
}

func polyContCheck(ax float64, ay float64, bx float64, by float64, cx float64, cy float64) int8 {
	EPSILON := 0.00000001
	if ay == by && ay == cy {
		if !((bx <= ax && ax <= cx) ||
			(cx <= ax && ax <= bx)) {
			return 1
		}
		return 0
	}
	if math.Abs(ay-by) < EPSILON &&
		math.Abs(ax-by) < EPSILON {
		return 0
	}

	if by > cy {
		tmpx := bx
		tmpy := by
		bx = cx
		by = cy
		cx = tmpx
		cy = tmpy
	}

	if ay <= by || ay > cy {
		return 1
	}

	d := (bx-ax)*(cy-ay) -
		(by-ay)*(cx-ax)

	if d > 0 {
		return -1
	}
	if d < 0 {
		return 1
	}
	return 0
}

func (feed *Feed) getGTFSDir(zip *zip.ReadCloser) string {
	// count number of GTFS file occurances in folders,
	// return the folder with the most GTFS files

	pathm := make(map[string]int)
	files := map[string]bool{
		"agency.txt":          true,
		"stops.txt":           true,
		"routes.txt":          true,
		"trips.txt":           true,
		"stop_times.txt":      true,
		"calendar.txt":        true,
		"calendar_dates.txt":  true,
		"fare_attributes.txt": true,
		"fare_rules.txt":      true,
		"shapes.txt":          true,
		"frequencies.txt":     true,
		"transfers.txt":       true,
		"pathways.txt":        true,
		"levels.txt":          true,
		"feed_info.txt":       true,
	}

	for _, f := range feed.zipFileCloser.File {
		dir, name := opath.Split(f.Name)
		if files[name] {
			pathm[dir] = pathm[dir] + 1
		}
	}

	ret := ""
	max := 0
	for dir := range pathm {
		if pathm[dir] > max {
			max = pathm[dir]
			ret = dir
		}
	}

	return ret
}

func (feed *Feed) warn(e error) {
	if feed.opts.ShowWarnings {
		fmt.Fprintln(os.Stderr, "WARNING: "+e.Error())
	}
}
