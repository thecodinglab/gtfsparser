// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfsparser

import (
	hex "encoding/hex"
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser/gtfs"
	"math"
	mail "net/mail"
	url "net/url"
	"strconv"
	"strings"
)

// custom error types for later checking
type StopNotFoundErr struct {
	prefix string
	sid    string
}

func (e *StopNotFoundErr) Error() string {
	return "No stop with id " + e.sid + " found."
}

func (e *StopNotFoundErr) StopId() string {
	return e.prefix + e.sid
}

func createAttribution(r map[string]string, feed *Feed, prefix string) (attr *gtfs.Attribution, ag *gtfs.Agency, route *gtfs.Route, trip *gtfs.Trip, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Attribution)

	a.Id = prefix + getString("attribution_id", r, false, false, "")
	a.Organization_name = getString("organization_name", r, true, true, feed.opts.EmptyStringRepl)
	a.Is_producer = getBool("is_producer", r, false, false, feed.opts.UseDefValueOnError, feed)
	a.Is_operator = getBool("is_operator", r, false, false, feed.opts.UseDefValueOnError, feed)
	a.Is_authority = getBool("is_authority", r, false, false, feed.opts.UseDefValueOnError, feed)

	a.Url = getURL("attribution_url", r, false, feed.opts.UseDefValueOnError, feed)
	a.Email = getMail("attribution_email", r, false, feed.opts.UseDefValueOnError, feed)
	a.Phone = getString("attribution_phone", r, false, false, feed.opts.EmptyStringRepl)

	routeId := getString("route_id", r, false, false, "")
	agencyId := getString("agency_id", r, false, false, "")
	tripId := getString("trip_id", r, false, false, "")

	if !a.Is_producer && !a.Is_operator && !a.Is_authority {
		return nil, nil, nil, nil, errors.New("One of is_producer, is_operator or is_authority must be set!")
	}

	if (len(routeId) != 0 && len(agencyId) != 0) || (len(routeId) != 0 && len(tripId) != 0) || (len(tripId) != 0 && len(agencyId) != 0) {
		return nil, nil, nil, nil, errors.New("Only one of route_id, agency_id or trip_id can be set!")
	}

	if len(agencyId) > 0 {
		if val, ok := feed.Agencies[prefix+agencyId]; ok {
			ag = val
		} else {
			panic(fmt.Errorf("No agency with id %s found", agencyId))
		}
	}

	if len(routeId) > 0 {
		if val, ok := feed.Routes[prefix+routeId]; ok {
			route = val
		} else {
			panic(fmt.Errorf("No route with id %s found", routeId))
		}
	}

	if len(tripId) > 0 {
		if val, ok := feed.Trips[prefix+tripId]; ok {
			trip = val
		} else {
			panic(fmt.Errorf("No trip with id %s found", tripId))
		}
	}

	return a, ag, route, trip, nil
}

func createAgency(r map[string]string, feed *Feed, prefix string) (ag *gtfs.Agency, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Agency)

	a.Id = prefix + getString("agency_id", r, false, false, "")
	a.Name = getString("agency_name", r, true, true, feed.opts.EmptyStringRepl)
	a.Url = getURL("agency_url", r, true, feed.opts.UseDefValueOnError, feed)
	a.Timezone = getTimezone("agency_timezone", r, true, feed.opts.UseDefValueOnError, feed)
	a.Lang = getIsoLangCode("agency_lang", r, false, feed.opts.UseDefValueOnError, feed)
	a.Phone = getString("agency_phone", r, false, false, "")
	a.Fare_url = getURL("agency_fare_url", r, false, feed.opts.UseDefValueOnError, feed)
	a.Email = getMail("agency_email", r, false, feed.opts.UseDefValueOnError, feed)

	return a, nil
}

func createFeedInfo(r map[string]string, feed *Feed) (fi *gtfs.FeedInfo, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	f := new(gtfs.FeedInfo)

	f.Publisher_name = getString("feed_publisher_name", r, true, true, feed.opts.EmptyStringRepl)
	f.Publisher_url = getURL("feed_publisher_url", r, true, feed.opts.UseDefValueOnError, feed)
	f.Lang = getString("feed_lang", r, true, true, feed.opts.EmptyStringRepl)
	f.Start_date = getDate("feed_start_date", r, false, feed.opts.UseDefValueOnError, feed)
	f.End_date = getDate("feed_end_date", r, false, feed.opts.UseDefValueOnError, feed)
	f.Version = getString("feed_version", r, false, false, "")
	f.Contact_email = getMail("feed_contact_email", r, false, feed.opts.UseDefValueOnError, feed)
	f.Contact_url = getURL("feed_contact_url", r, false, feed.opts.UseDefValueOnError, feed)

	return f, nil
}

func createFrequency(r map[string]string, feed *Feed, prefix string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := gtfs.Frequency{}
	var trip *gtfs.Trip

	tripid := prefix + getString("trip_id", r, true, true, "")

	if val, ok := feed.Trips[tripid]; ok {
		trip = val
	} else {
		panic(errors.New("No trip with id " + r["trip_id"] + " found."))
	}

	a.Exact_times = getBool("exact_times", r, false, false, feed.opts.UseDefValueOnError, feed)
	a.Start_time = getTime("start_time", r)
	a.End_time = getTime("end_time", r)

	if a.Start_time.SecondsSinceMidnight() > a.End_time.SecondsSinceMidnight() {
		panic(errors.New("Frequency has start_time > end_time."))
	}

	a.Headway_secs = getPositiveInt("headway_secs", r, true)

	if !feed.opts.DryRun {
		trip.Frequencies = append(trip.Frequencies, a)
	}

	return nil
}

func createRoute(r map[string]string, feed *Feed, prefix string) (route *gtfs.Route, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Route)
	a.Id = prefix + getString("route_id", r, true, true, "")

	var aID = prefix + getString("agency_id", r, false, false, "")

	if len(aID) != len(prefix) {
		if val, ok := feed.Agencies[aID]; ok {
			a.Agency = val
		} else {
			if feed.opts.UseDefValueOnError {
				if len(feed.Agencies) == 1 {
					a.Agency = nil
				} else {
					return nil, errors.New("Agency with id " + getString("agency_id", r, false, false, "") + " not found or erroneous, cannot fall back to no agency as there is more than one agency in agency.txt.")
				}
			} else {
				return nil, errors.New("No agency with id " + getString("agency_id", r, false, false, "") + " found.")
			}
		}
	} else if len(prefix) == 0 && len(feed.Agencies) == 1 {
		// if no agency is specified and we only have one agency in agencies.txt, use it here
		for _, ag := range feed.Agencies {
			a.Agency = ag
			break
		}
	} else if len(prefix) > 0 {
		c := 0
		aId := ""
		// if no agency is specified and we only have one agency in agencies.txt, use it here
		for _, ag := range feed.Agencies {
			if strings.HasPrefix(ag.Id, prefix) {
				aId = ag.Id
				c += 1
			}
		}

		if c == 1 {
			a.Agency = feed.Agencies[aId]
		} else {
			return nil, errors.New("No agency given for route " + a.Id + ", an agency is required as there is more than one agency in agency.txt.")
		}
	} else {
		return nil, errors.New("No agency given for route " + a.Id + ", an agency is required as there is more than one agency in agency.txt.")
	}

	a.Short_name = getString("route_short_name", r, false, false, "")
	a.Long_name = getString("route_long_name", r, false, false, "")

	if len(a.Short_name) == 0 && len(a.Long_name) == 0 {
		return nil, errors.New("Either route_short_name or route_long_name are required.")
	}

	a.Desc = getString("route_desc", r, false, false, "")
	a.Type = int16(getRangeInt("route_type", r, true, 0, 1702)) // allow extended route types
	a.Url = getURL("route_url", r, false, feed.opts.UseDefValueOnError, feed)
	a.Color = getColor("route_color", r, false, "ffffff", feed.opts.UseDefValueOnError, feed)
	a.Text_color = getColor("route_text_color", r, false, "000000", feed.opts.UseDefValueOnError, feed)
	a.Sort_order = getPositiveIntWithDefault("route_sort_order", r, -1, feed.opts.UseDefValueOnError, feed)

	return a, nil
}

func createServiceFromCalendar(r map[string]string, feed *Feed, prefix string) (s *gtfs.Service, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	service := new(gtfs.Service)
	service.Id = prefix + getString("service_id", r, true, true, "")
	service.Exceptions = make(map[gtfs.Date]bool, 0)

	// fill daybitmap
	service.Daymap[1] = getBool("monday", r, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[2] = getBool("tuesday", r, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[3] = getBool("wednesday", r, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[4] = getBool("thursday", r, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[5] = getBool("friday", r, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[6] = getBool("saturday", r, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[0] = getBool("sunday", r, true, false, feed.opts.UseDefValueOnError, feed)
	service.Start_date = getDate("start_date", r, true, false, feed)
	service.End_date = getDate("end_date", r, true, false, feed)

	if service.End_date.GetTime().Before(service.Start_date.GetTime()) {
		return nil, errors.New("Service " + getString("service_id", r, true, true, "") + " has the end date before the start date.")
	}

	return service, nil
}

func createServiceFromCalendarDates(r map[string]string, feed *Feed, filterDateStart gtfs.Date, filterDateEnd gtfs.Date, prefix string) (s *gtfs.Service, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	update := false
	var service *gtfs.Service

	// first, check if the service already exists
	if val, ok := feed.Services[prefix+getString("service_id", r, true, true, "")]; ok {
		service = val
		update = true
	} else {
		service = new(gtfs.Service)
		service.Id = prefix + getString("service_id", r, true, true, "")
		service.Exceptions = make(map[gtfs.Date]bool, 0)
	}

	// create exception
	t := getRangeInt("exception_type", r, true, 1, 2)
	date := getDate("date", r, true, false, feed)

	// may be nil during dry run
	if service != nil {
		if _, ok := service.Exceptions[date]; ok {
			return nil, errors.New("Date exception for service id " + getString("service_id", r, true, true, "") + " defined 2 times for one date.")
		}
		if (filterDateEnd.Year == 0 || !date.GetTime().After(filterDateEnd.GetTime())) &&
			(filterDateStart.Year == 0 || !date.GetTime().Before(filterDateStart.GetTime())) {
			service.SetExceptionTypeOn(date, int8(t))
		}
	}

	if update {
		return nil, nil
	}
	return service, nil
}

func createStop(r map[string]string, feed *Feed, prefix string) (s *gtfs.Stop, pid string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Stop)
	parentId := ""

	a.Id = prefix + getString("stop_id", r, true, true, "")
	a.Code = getString("stop_code", r, false, false, "")
	a.Location_type = int8(getRangeIntWithDefault("location_type", r, 0, 4, 0, feed.opts.UseDefValueOnError, feed))
	a.Name = getString("stop_name", r, a.Location_type < 3, a.Location_type < 3, feed.opts.EmptyStringRepl)
	a.Desc = getString("stop_desc", r, false, false, "")

	if a.Location_type < 3 {
		a.Lat = getFloat("stop_lat", r, true)
		a.Lon = getFloat("stop_lon", r, true)
	} else {
		lat := getNullableFloat("stop_lat", r, feed.opts.UseDefValueOnError, feed)
		lon := getNullableFloat("stop_lon", r, feed.opts.UseDefValueOnError, feed)

		if !math.IsNaN(float64(lat)) && !math.IsNaN(float64(lon)) {
			a.Lat = lat
			a.Lon = lon
		} else if !math.IsNaN(float64(lat)) {
			locErr := fmt.Errorf("stop_lat and stop_lon are optional for location_type=%d, but only stop_lon was ommitted here, and stop_lat was defined.", a.Location_type)
			if feed.opts.UseDefValueOnError {
				feed.warn(locErr)
				a.Lat = float32(math.NaN())
				a.Lon = float32(math.NaN())
			} else {
				panic(locErr)
			}
		} else if !math.IsNaN(float64(lon)) {
			locErr := fmt.Errorf("stop_lat and stop_lon are optional for location_type=%d, but only stop_lat was ommitted here, and stop_lon was defined.", a.Location_type)
			if feed.opts.UseDefValueOnError {
				feed.warn(locErr)
				a.Lat = float32(math.NaN())
				a.Lon = float32(math.NaN())
			} else {
				panic(locErr)
			}
		} else {
			a.Lat = float32(math.NaN())
			a.Lon = float32(math.NaN())
		}
	}

	// check for incorrect coordinates
	if a.HasLatLon() && math.Abs(float64(a.Lat)) > 90 {
		panic(fmt.Errorf("Expected coordinate (lat, lon), instead found (%f, %f), latitude is not in the allowed range [-90, 90].", a.Lat, a.Lon))
	}

	if a.HasLatLon() && math.Abs(float64(a.Lon)) > 180 {
		panic(fmt.Errorf("Expected coordinate (lat, lon), instead found (%f, %f), longitude is not in the allowed range [-180, 180].", a.Lat, a.Lon))
	}

	// check for 0,0 coordinates, which are most definitely an error
	if a.HasLatLon() && feed.opts.CheckNullCoordinates && math.Abs(float64(a.Lat)) < 0.0001 && math.Abs(float64(a.Lon)) < 0.0001 {
		panic(fmt.Errorf("Expected coordinate (lat, lon), instead found (0, 0), which is in the middle of the atlantic."))
	}

	a.Zone_id = prefix + getString("zone_id", r, false, false, "")
	if len(a.Zone_id) == len(prefix) {
		a.Zone_id = ""
	}
	a.Url = getURL("stop_url", r, false, feed.opts.UseDefValueOnError, feed)

	// will be filled later on
	a.Parent_station = nil

	if a.Location_type > 1 {
		parentId = prefix + getString("parent_station", r, true, true, "")
	} else if a.Location_type == 0 {
		parentId = prefix + getString("parent_station", r, false, false, "")
	} else {
		if len(getString("parent_station", r, false, false, "")) > 0 {
			panic(fmt.Errorf("'parent_station' cannot be defined for location_type=1."))
		}
	}

	a.Timezone = getTimezone("stop_timezone", r, false, feed.opts.UseDefValueOnError, feed)
	a.Wheelchair_boarding = int8(getRangeIntWithDefault("wheelchair_boarding", r, 0, 2, 0, feed.opts.UseDefValueOnError, feed))
	a.Level = nil

	levelId := prefix + getString("level_id", r, false, false, "")

	if len(levelId) > len(prefix) {
		if val, ok := feed.Levels[levelId]; ok {
			a.Level = val
		} else {
			panic(errors.New("No level with id " + getString("level_id", r, false, true, "") + " found."))
		}
	}

	a.Platform_code = getString("platform_code", r, false, false, "")

	return a, parentId, nil
}

func createStopTime(r map[string]string, feed *Feed, prefix string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := gtfs.StopTime{}
	var trip *gtfs.Trip

	if val, ok := feed.Trips[prefix+getString("trip_id", r, true, true, "")]; ok {
		trip = val
	} else {
		panic(errors.New("No trip with id " + getString("trip_id", r, true, true, "") + " found."))
	}

	if val, ok := feed.Stops[prefix+getString("stop_id", r, true, true, "")]; ok {
		a.Stop = val
	} else {
		panic(&StopNotFoundErr{prefix, getString("stop_id", r, true, true, "")})
	}

	if a.Stop.Location_type != 0 {
		panic(errors.New("Stop " + a.Stop.Id + " (" + a.Stop.Name + ") has location_type != 0, cannot be used in stop_times.txt!"))
	}

	a.Arrival_time = getTime("arrival_time", r)
	a.Departure_time = getTime("departure_time", r)

	if a.Arrival_time.Empty() && !a.Departure_time.Empty() {
		if feed.opts.UseDefValueOnError {
			a.Arrival_time = a.Departure_time
		} else {
			panic(errors.New("Missing arrival time for " + getString("stop_id", r, true, true, "") + "."))
		}
	}

	if !a.Arrival_time.Empty() && a.Departure_time.Empty() {
		if feed.opts.UseDefValueOnError {
			a.Departure_time = a.Arrival_time
		} else {
			panic(errors.New("Missing departure time for " + getString("stop_id", r, true, true, "") + "."))
		}
	}

	if a.Arrival_time.SecondsSinceMidnight() > a.Departure_time.SecondsSinceMidnight() {
		panic(errors.New("Departure before arrival at stop " + getString("stop_id", r, true, true, "") + "."))
	}

	a.Sequence = getPositiveInt("stop_sequence", r, true)
	a.Headsign = getString("stop_headsign", r, false, false, "")
	a.Pickup_type = int8(getRangeInt("pickup_type", r, false, 0, 3))
	a.Drop_off_type = int8(getRangeInt("drop_off_type", r, false, 0, 3))
	dist := getNullableFloat("shape_dist_traveled", r, feed.opts.UseDefValueOnError, feed)
	a.Shape_dist_traveled = dist
	a.Timepoint = getBool("timepoint", r, false, !a.Arrival_time.Empty() && !a.Departure_time.Empty(), feed.opts.UseDefValueOnError, feed)

	if (a.Arrival_time.Empty() || a.Departure_time.Empty()) && a.Timepoint {
		locErr := errors.New("Stops with timepoint=1 cannot have empty arrival or departure time")
		if feed.opts.UseDefValueOnError {
			a.Timepoint = false
			feed.warn(locErr)
		} else if !feed.opts.DropErroneous {
			panic(locErr)
		}
		feed.warn(locErr)
	}

	if checkStopTimesOrdering(a.Sequence, trip.StopTimes) {
		trip.StopTimes = append(trip.StopTimes, a)
	} else {
		locErr := errors.New("Stop time sequence collision. Sequence has to increase along trip")
		if !feed.opts.DropErroneous {
			panic(locErr)
		} else {
			feed.warn(locErr)
		}
	}

	return nil
}

func createTrip(r map[string]string, feed *Feed, prefix string) (t *gtfs.Trip, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Trip)
	a.Id = prefix + getString("trip_id", r, true, true, "")

	if val, ok := feed.Routes[prefix+getString("route_id", r, true, true, "")]; ok {
		a.Route = val
	} else {
		panic(fmt.Errorf("No route with id %s found", getString("route_id", r, true, true, "")))
	}

	if val, ok := feed.Services[prefix+getString("service_id", r, true, true, "")]; ok {
		a.Service = val
	} else {
		panic(fmt.Errorf("No service with id %s found", getString("service_id", r, true, true, "")))
	}

	a.Headsign = getString("trip_headsign", r, false, false, "")
	a.Short_name = getString("trip_short_name", r, false, false, "")
	a.Direction_id = int8(getRangeInt("direction_id", r, false, 0, 1))
	a.Block_id = prefix + getString("block_id", r, false, false, "")
	if len(a.Block_id) == len(prefix) {
		a.Block_id = ""
	}

	if !feed.opts.DropShapes {
		shapeID := prefix + getString("shape_id", r, false, false, "")

		if len(shapeID) > len(prefix) {
			if val, ok := feed.Shapes[shapeID]; ok {
				a.Shape = val
			} else {
				locErr := fmt.Errorf("No shape with id %s found", shapeID)
				if feed.opts.UseDefValueOnError {
					feed.warn(locErr)
					a.Shape = nil
				} else {
					return nil, locErr
				}
			}
		}
	}

	a.Wheelchair_accessible = int8(getRangeIntWithDefault("wheelchair_accessible", r, 0, 2, 0, feed.opts.UseDefValueOnError, feed))
	a.Bikes_allowed = int8(getRangeIntWithDefault("bikes_allowed", r, 0, 2, 0, feed.opts.UseDefValueOnError, feed))

	return a, nil
}

func createShapePoint(r map[string]string, feed *Feed, prefix string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	shapeID := prefix + getString("shape_id", r, true, true, "")
	var shape *gtfs.Shape

	if val, ok := feed.Shapes[shapeID]; ok {
		shape = val
	} else {
		// create new shape
		shape = new(gtfs.Shape)
		shape.Id = shapeID
		// push it onto the shape map
		feed.Shapes[shapeID] = shape
	}
	dist := getNullableFloat("shape_dist_traveled", r, feed.opts.UseDefValueOnError, feed)

	lat := getFloat("shape_pt_lat", r, true)
	lon := getFloat("shape_pt_lon", r, true)

	// check for incorrect coordinates
	if math.Abs(float64(lat)) > 90 {
		panic(fmt.Errorf("Expected coordinate (lat, lon), instead found (%f, %f), latitude is not in the allowed range [-90, 90].", lat, lon))
	}

	if math.Abs(float64(lon)) > 180 {
		panic(fmt.Errorf("Expected coordinate (lat, lon), instead found (%f, %f), longitude is not in the allowed range [-180, 180].", lat, lon))
	}

	// check for 0,0 coordinates, which are most definitely an error
	if feed.opts.CheckNullCoordinates && math.Abs(float64(lat)) < 0.0001 && math.Abs(float64(lon)) < 0.0001 {
		panic(fmt.Errorf("Expected coordinate (lat, lon), instead found (0, 0), which is in the middle of the atlantic."))
	}

	p := gtfs.ShapePoint{
		Lat:           lat,
		Lon:           lon,
		Sequence:      getInt("shape_pt_sequence", r, true),
		Dist_traveled: dist,
	}

	if checkShapePointOrdering(p.Sequence, shape.Points) {
		shape.Points = append(shape.Points, p)
	} else {
		locErr := errors.New("Shape point sequence collision. Sequence has to increase along shape")
		if !feed.opts.DropErroneous {
			panic(locErr)
		} else {
			feed.warn(locErr)
		}
	}

	return nil
}

func createFareAttribute(r map[string]string, feed *Feed, prefix string) (fa *gtfs.FareAttribute, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.FareAttribute)

	a.Id = prefix + getString("fare_id", r, true, true, "")
	a.Price = getString("price", r, false, false, "")
	if feed.opts.UseDefValueOnError {
		a.Currency_type = getString("currency_type", r, true, true, "XXX")
	} else {
		a.Currency_type = getString("currency_type", r, true, true, "")
	}
	a.Payment_method = getRangeInt("payment_method", r, false, 0, 1)
	a.Transfers = getRangeIntWithDefault("transfers", r, 0, 2, -1, feed.opts.UseDefValueOnError, feed)
	a.Transfer_duration = getInt("transfer_duration", r, false)

	aID := prefix + getString("agency_id", r, false, false, "")

	if len(aID) != len(prefix) {
		if val, ok := feed.Agencies[aID]; ok {
			a.Agency = val
		} else {
			if feed.opts.UseDefValueOnError {
				a.Agency = nil
			} else {
				return nil, errors.New("No agency with id " + getString("agency_id", r, false, false, "") + " found.")
			}
		}
	} else if len(feed.Agencies) > 1 {
		return nil, errors.New("Expected a non-empty value for 'agency_id', as there are multiple agencies defined in agency.txt.")
	}

	return a, nil
}

func createFareRule(r map[string]string, feed *Feed, prefix string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	var fareattr *gtfs.FareAttribute
	var fareid string

	fareid = prefix + getString("fare_id", r, true, true, "")

	// first, check if the service already exists
	if val, ok := feed.FareAttributes[fareid]; ok {
		fareattr = val
	} else {
		panic(fmt.Errorf("No fare attribute with id %s found", fareid))
	}

	// create fare attribute
	rule := new(gtfs.FareAttributeRule)

	routeID := prefix + getString("route_id", r, false, false, "")

	if len(routeID) > len(prefix) {
		if val, ok := feed.Routes[routeID]; ok {
			rule.Route = val
		} else {
			panic(fmt.Errorf("No route with id %s found", routeID))
		}
	}

	rule.Origin_id = prefix + getString("origin_id", r, false, false, "")
	rule.Destination_id = prefix + getString("destination_id", r, false, false, "")
	rule.Contains_id = prefix + getString("contains_id", r, false, false, "")

	fareattr.Rules = append(fareattr.Rules, rule)

	return nil
}

func createTransfer(r map[string]string, feed *Feed, prefix string) (t *gtfs.Transfer, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Transfer)

	if val, ok := feed.Stops[prefix+getString("from_stop_id", r, true, true, "")]; ok {
		a.From_stop = val
	} else {
		panic(&StopNotFoundErr{prefix, getString("from_stop_id", r, true, true, "")})
	}

	if val, ok := feed.Stops[prefix+getString("to_stop_id", r, true, true, "")]; ok {
		a.To_stop = val
	} else {
		panic(&StopNotFoundErr{prefix, getString("to_stop_id", r, true, true, "")})
	}

	a.Transfer_type = getRangeInt("transfer_type", r, false, 0, 3)
	a.Min_transfer_time = getPositiveIntWithDefault("min_transfer_time", r, -1, feed.opts.UseDefValueOnError, feed)

	return a, nil
}

func createPathway(r map[string]string, feed *Feed, prefix string) (t *gtfs.Pathway, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Pathway)

	a.Id = prefix + getString("pathway_id", r, true, true, "")

	if val, ok := feed.Stops[prefix+getString("from_stop_id", r, true, true, "")]; ok {
		a.From_stop = val
		if a.From_stop.Location_type == 1 {
			panic(errors.New("Stop for 'from_stop_id' with id " + getString("from_stop_id", r, true, true, "") + " has location_type=1 (Station). Only stops/platforms (location_type=0), entrances/exits (location_type=2), generic nodes (location_type=3) or boarding areas (location_type=4) are allowed here."))
		}
	} else {
		panic(&StopNotFoundErr{prefix, getString("from_stop_id", r, true, true, "")})
	}

	if val, ok := feed.Stops[prefix+getString("to_stop_id", r, true, true, "")]; ok {
		a.To_stop = val
		if a.To_stop.Location_type == 1 {
			panic(errors.New("Stop for 'to_stop_id' with id " + getString("to_stop_id", r, true, true, "") + " has location_type=1 (Station). Only stops/platforms (location_type=0), entrances/exits (location_type=2), generic nodes (location_type=3) or boarding areas (location_type=4) are allowed here."))
		}
	} else {
		panic(&StopNotFoundErr{prefix, getString("to_stop_id", r, true, true, "")})
	}

	a.Mode = uint8(getRangeInt("pathway_mode", r, true, 1, 7))
	a.Is_bidirectional = getBool("is_bidirectional", r, true, false, feed.opts.UseDefValueOnError, feed)

	length := getNullableFloat("length", r, feed.opts.UseDefValueOnError, feed)
	a.Length = length

	a.Traversal_time = int(getPositiveIntWithDefault("traversal_time", r, -1, feed.opts.UseDefValueOnError, feed))

	a.Stair_count = getIntWithDefault("stair_count", r, 0, feed.opts.UseDefValueOnError, feed)
	a.Max_slope = getNullableFloat("max_slope", r, feed.opts.UseDefValueOnError, feed)
	if math.IsNaN(float64(a.Max_slope)) {
		a.Max_slope = 0
	}

	width := getNullablePositiveFloat("min_width", r, feed.opts.UseDefValueOnError, feed)
	a.Min_width = width

	a.Signposted_as = getString("signposted_as", r, false, false, "")
	a.Reversed_signposted_as = getString("reversed_signposted_as", r, false, false, "")

	return a, nil
}

func createLevel(r map[string]string, feed *Feed, idprefix string) (t *gtfs.Level, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Level)

	a.Id = idprefix + getString("level_id", r, true, true, "")
	a.Index = getNullableFloat("level_index", r, feed.opts.UseDefValueOnError, feed)
	if math.IsNaN(float64(a.Index)) {
		a.Index = 0
	}
	a.Name = getString("level_name", r, false, false, "")

	return a, nil
}

func getString(name string, r map[string]string, req bool, nonempty bool, emptyrepl string) string {
	if val, ok := r[name]; ok {
		trimmed := strings.TrimSpace(val)
		if nonempty && len(trimmed) == 0 {
			if len(emptyrepl) > 0 {
				return emptyrepl
			} else {
				panic(fmt.Errorf("Expected non-empty string for field '%s'", name))
			}
		} else {
			return trimmed
		}
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return ""
}

func getURL(name string, r map[string]string, req bool, ignErrs bool, feed *Feed) *url.URL {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		u, e := url.ParseRequestURI(strings.TrimSpace(val))
		if e != nil {
			locErr := fmt.Errorf("'%s' is not a valid url", errFldPrep(val))
			if req || !ignErrs {
				panic(locErr)
			} else {
				feed.warn(locErr)
				return nil
			}
		}
		return u
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return nil
}

func getMail(name string, r map[string]string, req bool, ignErrs bool, feed *Feed) *mail.Address {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		u, e := mail.ParseAddress(strings.TrimSpace(val))
		if e != nil {
			locErr := fmt.Errorf("'%s' is not a valid email address", errFldPrep(val))
			if req || !ignErrs {
				panic(locErr)
			} else {
				feed.warn(locErr)
				return nil
			}
		}
		return u
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return nil
}

func getTimezone(name string, r map[string]string, req bool, ignErrs bool, feed *Feed) gtfs.Timezone {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		tz, e := gtfs.NewTimezone(strings.TrimSpace(val))
		if e != nil && (req || !ignErrs) {
			panic(e)
		} else if e != nil {
			feed.warn(e)
			return tz
		}
		return tz
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	tz, _ := gtfs.NewTimezone("")
	return tz
}

func getIsoLangCode(name string, r map[string]string, req bool, ignErrs bool, feed *Feed) gtfs.LanguageISO6391 {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		l, e := gtfs.NewLanguageISO6391(strings.TrimSpace(val))
		if e != nil && (req || !ignErrs) {
			panic(e)
		} else if e != nil {
			feed.warn(e)
			return l
		}
		return l
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	l, _ := gtfs.NewLanguageISO6391("")
	return l
}

func getColor(name string, r map[string]string, req bool, def string, ignErrs bool, feed *Feed) string {
	if val, ok := r[name]; ok && len(val) > 0 {
		val = strings.TrimSpace(val)
		if len(val) != 6 {
			locErr := fmt.Errorf("Expected six-character hexadecimal number as color for field '%s' (found: %s)", name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}

		if _, e := hex.DecodeString(val); e != nil {
			locErr := fmt.Errorf("Expected hexadecimal number as color for field '%s' (found: %s)", name, val)
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}
		return strings.ToUpper(val)
	} else if req {
		locErr := fmt.Errorf("Expected required field '%s'", name)
		if ignErrs {
			feed.warn(locErr)
			return def
		}
		panic(locErr)
	}
	return strings.ToUpper(def)
}

func getInt(name string, r map[string]string, req bool) int {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			panic(fmt.Errorf("Expected integer for field '%s', found '%s'", name, errFldPrep(val)))
		}
		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return 0
}

func getIntWithDefault(name string, r map[string]string, def int, ignErrs bool, feed *Feed) int {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			locErr := fmt.Errorf("Expected integer for field '%s', found '%s'", name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}
		return num
	}
	return def
}

func getPositiveInt(name string, r map[string]string, req bool) int {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || num < 0 {
			panic(fmt.Errorf("Expected positive integer for field '%s', found '%s'", name, errFldPrep(val)))
		}
		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return 0
}

func getPositiveIntWithDefault(name string, r map[string]string, def int, ignErrs bool, feed *Feed) int {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || num < 0 {
			locErr := fmt.Errorf("Expected positive integer for field '%s', found '%s'", name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}
		return num
	}
	return def
}

func getRangeInt(name string, r map[string]string, req bool, min int, max int) int {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			panic(fmt.Errorf("Expected integer for field '%s', found '%s'", name, errFldPrep(val)))
		}

		if num > max || num < min {
			panic(fmt.Errorf("Expected integer between %d and %d for field '%s', found %s", min, max, name, errFldPrep(val)))
		}

		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return 0
}

func getRangeIntWithDefault(name string, r map[string]string, min int, max int, def int, ignErrs bool, feed *Feed) int {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			locErr := fmt.Errorf("Expected integer for field '%s', found '%s'", name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}

		if num > max || num < min {
			locErr := fmt.Errorf("Expected integer between %d and %d for field '%s', found %s", min, max, name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}

		return num
	}
	return def
}

func getFloat(name string, r map[string]string, req bool) float32 {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		trimmed := strings.TrimSpace(val)
		num, err := strconv.ParseFloat(trimmed, 32)
		if err != nil {
			panic(fmt.Errorf("Expected float for field '%s', found '%s'", name, errFldPrep(val)))
		}
		return float32(num)
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return -1
}

func getNullablePositiveFloat(name string, r map[string]string, ignErrs bool, feed *Feed) float32 {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.ParseFloat(strings.TrimSpace(val), 32)
		if err != nil || num < 0 {
			locErr := fmt.Errorf("Expected positive float for field '%s', found '%s'", name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return float32(math.NaN())
			}
			panic(locErr)
		}
		return float32(num)
	}
	return float32(math.NaN())
}

func getNullableFloat(name string, r map[string]string, ignErrs bool, feed *Feed) float32 {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.ParseFloat(strings.TrimSpace(val), 32)
		if err != nil {
			locErr := fmt.Errorf("Expected float for field '%s', found '%s'", name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return float32(math.NaN())
			}
			panic(locErr)
		}
		return float32(num)
	}
	return float32(math.NaN())
}

func getBool(name string, r map[string]string, req bool, def bool, ignErrs bool, feed *Feed) bool {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || (num != 0 && num != 1) {
			locErr := fmt.Errorf("Expected 1 or 0 for field '%s', found '%s'", name, errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}
		return num == 1
	} else if req {
		locErr := fmt.Errorf("Expected required field '%s'", name)
		if ignErrs {
			feed.warn(locErr)
			return def
		}
		panic(locErr)
	}
	return def
}

func getDate(name string, r map[string]string, req bool, ignErrs bool, feed *Feed) gtfs.Date {
	var str string
	var ok bool
	if str, ok = r[name]; !ok || len(str) == 0 {
		locErr := fmt.Errorf("Expected required field '%s'", name)
		if req {
			panic(locErr)
		} else {
			feed.warn(locErr)
			return gtfs.Date{Day: 0, Month: 0, Year: 0}
		}
	}

	var day, month, year int
	var e error
	if len(str) < 8 {
		e = fmt.Errorf("only has %d characters, expected 8", len(str))
	}
	if e == nil {
		day, e = strconv.Atoi(str[6:8])
	}
	if e == nil {
		month, e = strconv.Atoi(str[4:6])
	}
	if e == nil {
		year, e = strconv.Atoi(str[0:4])
	}

	if e != nil {
		locErr := fmt.Errorf("Expected YYYYMMDD date for field '%s', found '%s' (%s)", name, errFldPrep(str), e.Error())
		if !ignErrs {
			panic(locErr)
		}
		feed.warn(locErr)
	}
	return gtfs.Date{Day: int8(day), Month: int8(month), Year: int16(year)}
}

func getTime(name string, r map[string]string) gtfs.Time {
	var str string
	var ok bool
	if str, ok = r[name]; !ok {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}

	str = strings.TrimSpace(str)

	if len(str) == 0 {
		return gtfs.Time{Second: int8(-1), Minute: int8(-1), Hour: int8(-1)}
	}

	var hour, minute, second int
	parts := strings.Split(str, ":")
	var e error

	if len(parts) != 3 || len(parts[0]) == 0 || len(parts[1]) != 2 || len(parts[2]) != 2 {
		e = fmt.Errorf("Expected HH:MM:SS time for field '%s', found '%s' (%s)", name, errFldPrep(str), e.Error())
	}

	if e == nil {
		hour, e = strconv.Atoi(parts[0])
	}
	if e == nil {
		minute, e = strconv.Atoi(parts[1])
	}
	if e == nil {
		second, e = strconv.Atoi(parts[2])
	}

	if e != nil {
		panic(fmt.Errorf("Expected HH:MM:SS time for field '%s', found '%s' (%s)", name, errFldPrep(str), e.Error()))
	} else {
		return gtfs.Time{Hour: int8(hour), Minute: int8(minute), Second: int8(second)}
	}
}

func checkShapePointOrdering(seq int, sts gtfs.ShapePoints) bool {
	for _, st := range sts {
		if seq == st.Sequence {
			return false
		}
	}

	return true
}

func checkStopTimesOrdering(seq int, sts gtfs.StopTimes) bool {
	for _, st := range sts {
		if seq == st.Sequence {
			return false
		}
	}

	return true
}

func errFldPrep(val string) string {
	a := strings.Replace(val, "\r", "<CR>", -1)
	a = strings.Replace(a, "\n", "<LF>", -1)
	a = strings.Replace(a, "\025", "<NL>", -1)
	return a
}
