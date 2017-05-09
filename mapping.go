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
	"strconv"
	"strings"
)

func createAgency(r map[string]string) (ag *gtfs.Agency, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Agency)

	a.Id = getString("agency_id", r, false)
	a.Name = getString("agency_name", r, true)
	a.Url = getString("agency_url", r, true)
	a.Timezone = getString("agency_timezone", r, true)
	a.Lang = getString("agency_lang", r, false)
	a.Phone = getString("agency_phone", r, false)
	a.Fare_url = getString("agency_fare_url", r, false)
	a.Email = getString("agency_email", r, false)

	return a, nil
}

func createFeedInfo(r map[string]string, opts *ParseOptions) (fi *gtfs.FeedInfo, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	f := new(gtfs.FeedInfo)

	f.Publisher_name = getString("feed_publisher_name", r, true)
	f.Publisher_url = getString("feed_publisher_url", r, true)
	f.Lang = getString("feed_lang", r, true)
	f.Start_date = getDate("feed_start_date", r, false, opts.UseDefValueOnError)
	f.End_date = getDate("feed_end_date", r, false, opts.UseDefValueOnError)
	f.Version = getString("feed_version", r, false)

	return f, nil
}

func createFrequency(r map[string]string, trips map[string]*gtfs.Trip, opts *ParseOptions) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := gtfs.Frequency{}
	var trip *gtfs.Trip

	tripid := getString("trip_id", r, true)

	if val, ok := trips[tripid]; ok {
		trip = val
	} else {
		panic(errors.New("No trip with id " + r["trip_id"] + " found."))
	}

	a.Exact_times = getBool("exact_times", r, false, false, opts.UseDefValueOnError)
	a.Start_time = getTime("start_time", r)
	a.End_time = getTime("end_time", r)
	a.Headway_secs = getPositiveInt("headway_secs", r, true)
	trip.Frequencies = append(trip.Frequencies, a)

	return nil
}

func createRoute(r map[string]string, agencies map[string]*gtfs.Agency, opts *ParseOptions) (route *gtfs.Route, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Route)
	a.Id = getString("route_id", r, true)

	var aId = getString("agency_id", r, false)

	if len(aId) != 0 {
		if val, ok := agencies[aId]; ok {
			a.Agency = val
		} else {
			if opts.UseDefValueOnError {
				a.Agency = nil
			} else {
				return nil, errors.New("No agency with id " + aId + " found.")
			}
		}
	}

	a.Short_name = getString("route_short_name", r, true)
	a.Long_name = getString("route_long_name", r, true)
	a.Desc = getString("route_desc", r, false)
	a.Type = getRangeInt("route_type", r, true, 0, 1702) // allow extended route types
	a.Url = getString("route_url", r, false)
	a.Color = getColor("route_color", r, false, "ffffff", opts.UseDefValueOnError)
	a.Text_color = getColor("route_text_color", r, false, "000000", opts.UseDefValueOnError)

	return a, nil
}

func createServiceFromCalendar(r map[string]string, services map[string]*gtfs.Service, opts *ParseOptions) (s *gtfs.Service, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	service := new(gtfs.Service)
	service.Id = getString("service_id", r, true)

	// fill daybitmap
	service.Daymap[1] = getBool("monday", r, true, false, opts.UseDefValueOnError)
	service.Daymap[2] = getBool("tuesday", r, true, false, opts.UseDefValueOnError)
	service.Daymap[3] = getBool("wednesday", r, true, false, opts.UseDefValueOnError)
	service.Daymap[4] = getBool("thursday", r, true, false, opts.UseDefValueOnError)
	service.Daymap[5] = getBool("friday", r, true, false, opts.UseDefValueOnError)
	service.Daymap[6] = getBool("saturday", r, true, false, opts.UseDefValueOnError)
	service.Daymap[0] = getBool("sunday", r, true, false, opts.UseDefValueOnError)
	service.Start_date = getDate("start_date", r, true, false)
	service.End_date = getDate("end_date", r, true, false)

	return service, nil
}

func createServiceFromCalendarDates(r map[string]string, services map[string]*gtfs.Service) (s *gtfs.Service, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	update := false
	var service *gtfs.Service

	// first, check if the service already exists
	if val, ok := services[getString("service_id", r, true)]; ok {
		service = val
		update = true
	} else {
		service = new(gtfs.Service)
		service.Id = getString("service_id", r, true)
	}

	// create exception
	exc := new(gtfs.ServiceException)
	var t int
	t = getRangeInt("exception_type", r, true, 1, 2)
	exc.Type = int8(t)
	exc.Date = getDate("date", r, true, false)

	service.Exceptions = append(service.Exceptions, exc)

	if update {
		return nil, nil
	} else {
		return service, nil
	}
}

func createStop(r map[string]string, opts *ParseOptions) (s *gtfs.Stop, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Stop)

	a.Id = getString("stop_id", r, true)
	a.Code = getString("stop_code", r, false)
	a.Name = getString("stop_name", r, true)
	a.Desc = getString("stop_desc", r, false)
	a.Lat = getFloat("stop_lat", r, true)
	a.Lon = getFloat("stop_lon", r, true)
	a.Zone_id = getString("zone_id", r, false)
	a.Url = getString("stop_url", r, false)
	a.Location_type = getRangeIntWithDefault("location_type", r, 0, 1, 0, opts.UseDefValueOnError)
	a.Parent_station = nil
	a.Timezone = getString("stop_timezone", r, false)
	a.Wheelchair_boarding = getRangeIntWithDefault("wheelchair_boarding", r, 0, 2, 0, opts.UseDefValueOnError)

	return a, nil
}

func createStopTime(r map[string]string, stops map[string]*gtfs.Stop, trips map[string]*gtfs.Trip, opts *ParseOptions) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := gtfs.StopTime{}
	var trip *gtfs.Trip

	if val, ok := trips[getString("trip_id", r, true)]; ok {
		trip = val
	} else {
		panic(errors.New("No trip with id " + getString("trip_id", r, true) + " found."))
	}

	if val, ok := stops[getString("stop_id", r, true)]; ok {
		a.Stop = val
	} else {
		panic(errors.New("No stop with id " + getString("stop_id", r, true) + " found."))
	}

	a.Arrival_time = getTime("arrival_time", r)
	a.Departure_time = getTime("departure_time", r)
	a.Sequence = getPositiveInt("stop_sequence", r, true)
	a.Headsign = getString("stop_headsign", r, false)
	a.Pickup_type = int8(getRangeInt("pickup_type", r, false, 0, 3))
	a.Drop_off_type = int8(getRangeInt("drop_off_type", r, false, 0, 3))
	dist, nulled := getNullableFloat("shape_dist_traveled", r, opts.UseDefValueOnError)
	a.Shape_dist_traveled = dist
	a.Has_dist = !nulled
	a.Timepoint = getBool("Timepoint", r, false, true, opts.UseDefValueOnError)

	if checkStopTimesOrdering(a.Sequence, trip.StopTimes) {
		trip.StopTimes = append(trip.StopTimes, a)
	} else if !opts.DropErroneous {
		panic(errors.New("Stop time sequence collision. Sequence has to increase along trip."))
	}

	return nil
}

func createTrip(r map[string]string, routes map[string]*gtfs.Route,
	services map[string]*gtfs.Service,
	shapes map[string]*gtfs.Shape, opts *ParseOptions) (t *gtfs.Trip, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Trip)
	a.Id = getString("trip_id", r, true)

	if val, ok := routes[getString("route_id", r, true)]; ok {
		a.Route = val
	} else {
		panic(errors.New(fmt.Sprintf("No route with id %s found", getString("route_id", r, true))))
	}

	if val, ok := services[getString("service_id", r, true)]; ok {
		a.Service = val
	} else {
		panic(errors.New(fmt.Sprintf("No service with id %s found", getString("service_id", r, true))))
	}

	a.Headsign = getString("trip_headsign", r, false)
	a.Short_name = getString("trip_short_name", r, false)
	a.Direction_id = int8(getRangeInt("direction_id", r, false, 0, 1))
	a.Block_id = getString("block_id", r, false)

	shapeId := getString("shape_id", r, false)

	if len(shapeId) > 0 {
		if val, ok := shapes[shapeId]; ok {
			a.Shape = val
		} else {
			if opts.UseDefValueOnError {
				a.Shape = nil
			} else {
				return nil, errors.New(fmt.Sprintf("No shape with id %s found", shapeId))
			}
		}
	}

	a.Wheelchair_accessible = int8(getRangeIntWithDefault("wheelchair_accessible", r, 0, 2, 0, opts.UseDefValueOnError))
	a.Bikes_allowed = int8(getRangeIntWithDefault("bikes_allowed", r, 0, 2, 0, opts.UseDefValueOnError))

	return a, nil
}

func createShapePoint(r map[string]string, shapes map[string]*gtfs.Shape, opts *ParseOptions) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	shapeId := getString("shape_id", r, true)
	var shape *gtfs.Shape

	if val, ok := shapes[shapeId]; ok {
		shape = val
	} else {
		// create new shape
		shape = new(gtfs.Shape)
		shape.Id = shapeId
		// push it onto the shape map
		shapes[shapeId] = shape
	}
	dist, nulled := getNullableFloat("shape_dist_traveled", r, opts.UseDefValueOnError)
	p := gtfs.ShapePoint{
		Lat:           getFloat("shape_pt_lat", r, true),
		Lon:           getFloat("shape_pt_lon", r, true),
		Sequence:      getInt("shape_pt_sequence", r, true),
		Dist_traveled: dist,
		Has_dist:      !nulled,
	}

	if checkShapePointOrdering(p.Sequence, shape.Points) {
		shape.Points = append(shape.Points, p)
	} else if !opts.DropErroneous {
		panic(errors.New("Shape point sequence collision. Sequence has to increase along shape."))
	}

	return nil
}

func createFareAttribute(r map[string]string, opts *ParseOptions) (fa *gtfs.FareAttribute, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.FareAttribute)

	a.Id = getString("fare_id", r, true)
	a.Price = getString("price", r, false)
	a.Currency_type = getString("currency_type", r, true)
	a.Payment_method = getRangeInt("payment_method", r, false, 0, 1)
	a.Transfers = getRangeIntWithDefault("transfers", r, 0, 2, -1, opts.UseDefValueOnError)
	a.Transfer_duration = getInt("transfer_duration", r, false)

	return a, nil
}

func createFareRule(r map[string]string, fareattributes map[string]*gtfs.FareAttribute, routes map[string]*gtfs.Route) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	var fareattr *gtfs.FareAttribute
	var fareid string

	fareid = getString("fare_id", r, true)

	// first, check if the service already exists
	if val, ok := fareattributes[fareid]; ok {
		fareattr = val
	} else {
		panic(errors.New(fmt.Sprintf("No fare attribute with id %s found", fareid)))
	}

	// create fare attribute
	rule := new(gtfs.FareAttributeRule)

	var route_id string
	route_id = getString("route_id", r, false)

	if len(route_id) > 0 {
		if val, ok := routes[route_id]; ok {
			rule.Route = val
		} else {
			panic(errors.New(fmt.Sprintf("No route with id %s found", route_id)))
		}
	}

	rule.Origin_id = getString("origin_id", r, false)
	rule.Destination_id = getString("destination_id", r, false)
	rule.Contains_id = getString("contains_id", r, false)

	fareattr.Rules = append(fareattr.Rules, rule)

	return nil
}

func createTransfer(r map[string]string, stops map[string]*gtfs.Stop, opts *ParseOptions) (t *gtfs.Transfer, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Transfer)

	if val, ok := stops[getString("from_stop_id", r, true)]; ok {
		a.From_stop = val
	} else {
		panic(errors.New("No stop with id " + getString("from_stop_id", r, true) + " found."))
	}

	if val, ok := stops[getString("to_stop_id", r, true)]; ok {
		a.To_stop = val
	} else {
		panic(errors.New("No stop with id " + getString("to_stop_id", r, true) + " found."))
	}

	a.Transfer_type = getRangeInt("transfer_type", r, true, 0, 3)
	a.Min_transfer_time = getPositiveIntWithDefault("min_transfer_time", r, -1, opts.UseDefValueOnError)

	return a, nil
}

func getString(name string, r map[string]string, req bool) string {
	if val, ok := r[name]; ok {
		return val
	} else if req {
		panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
	}
	return ""
}

func getColor(name string, r map[string]string, req bool, def string, ignErrs bool) string {
	if val, ok := r[name]; ok && len(val) > 0 {
		if len(val) != 6 {
			if ignErrs {
				return def
			} else {
				panic(errors.New(fmt.Sprintf("Expected six-character hexadecimal number as color for field '%s' (found: %s)", name, val)))
			}
		}

		if _, e := hex.DecodeString(val); e != nil {
			if ignErrs {
				return def
			} else {
				panic(errors.New(fmt.Sprintf("Expected hexadecimal number as color for field '%s' (found: %s)", name, val)))
			}
		}
		return strings.ToUpper(val)
	} else if req {
		if ignErrs {
			return def
		} else {
			panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
		}
	}
	return strings.ToUpper(def)
}

func getInt(name string, r map[string]string, req bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil {
			panic(errors.New(fmt.Sprintf("Expected integer for field '%s', found '%s'", name, val)))
		}
		return num
	} else if req {
		panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
	}
	return 0
}

func getPositiveInt(name string, r map[string]string, req bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil || num < 0 {
			panic(errors.New(fmt.Sprintf("Expected positive integer for field '%s', found '%s'", name, val)))
		}
		return num
	} else if req {
		panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
	}
	return 0
}

func getPositiveIntWithDefault(name string, r map[string]string, def int, ignErrs bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil || num < 0 {
			if ignErrs {
				return def
			} else {
				panic(errors.New(fmt.Sprintf("Expected positive integer for field '%s', found '%s'", name, val)))
			}
		}
		return num
	}
	return def
}

func getRangeInt(name string, r map[string]string, req bool, min int, max int) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil {
			panic(errors.New(fmt.Sprintf("Expected integer for field '%s', found '%s'", name, val)))
		}

		if num > max || num < min {
			panic(errors.New(fmt.Sprintf("Expected integer between %d and %d for field '%s', found %s", min, max, name, val)))
		}

		return num
	} else if req {
		panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
	}
	return 0
}

func getRangeIntWithDefault(name string, r map[string]string, min int, max int, def int, ignErrs bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil {
			if ignErrs {
				return def
			} else {
				panic(errors.New(fmt.Sprintf("Expected integer for field '%s', found '%s'", name, val)))
			}
		}

		if num > max || num < min {
			if ignErrs {
				return def
			} else {
				panic(errors.New(fmt.Sprintf("Expected integer between %d and %d for field '%s', found %s", min, max, name, val)))
			}
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
			panic(errors.New(fmt.Sprintf("Expected float for field '%s', found '%s'", name, val)))
		}
		return float32(num)
	} else if req {
		panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
	}
	return -1
}

func getNullableFloat(name string, r map[string]string, ignErrs bool) (float32, bool) {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.ParseFloat(strings.TrimSpace(val), 32)
		if err != nil {
			if ignErrs {
				return 0, true
			} else {
				panic(errors.New(fmt.Sprintf("Expected float for field '%s', found '%s'", name, val)))
			}
		}
		return float32(num), false
	}
	return 0, true
}

func getBool(name string, r map[string]string, req bool, def bool, ignErrs bool) bool {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil || (num != 0 && num != 1) {
			if ignErrs {
				return def
			} else {
				panic(errors.New(fmt.Sprintf("Expected 1 or 0 for field '%s', found '%s'", name, val)))
			}
		}
		return num == 1
	} else if req {
		if ignErrs {
			return def
		} else {
			panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
		}
	}
	return def
}

func getDate(name string, r map[string]string, req bool, ignErrs bool) gtfs.Date {
	var str string
	var ok bool
	if str, ok = r[name]; !ok || len(str) == 0 {
		if req {
			panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
		} else {
			return gtfs.Date{0, 0, 0}
		}
	}

	var day, month, year int
	var e error
	if len(str) < 8 {
		e = errors.New(fmt.Sprintf("only has %d characters, expected 8", len(str)))
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

	if e != nil && !ignErrs {
		panic(errors.New(fmt.Sprintf("Expected YYYYMMDD date for field '%s', found '%s' (%s)", name, str, e.Error())))
	} else {
		return gtfs.Date{int8(day), int8(month), int16(year)}
	}
}

func getTime(name string, r map[string]string) gtfs.Time {
	var str string
	var ok bool
	if str, ok = r[name]; !ok {
		panic(errors.New(fmt.Sprintf("Expected required field '%s'", name)))
	}

	var hour, minute, second int
	parts := strings.Split(str, ":")
	var e error

	if len(parts) != 3 || len(parts[0]) == 0 || len(parts[1]) != 2 || len(parts[2]) != 2 {
		e = errors.New(fmt.Sprintf("expected to be in HH:MM:SS format", len(str)))
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
		panic(errors.New(fmt.Sprintf("Expected HH:MM:SS time for field '%s', found '%s' (%s)", name, str, e.Error())))
	} else {
		return gtfs.Time{hour, int8(minute), int8(second)}
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
