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
	mail "net/mail"
	url "net/url"
	"strconv"
	"strings"
)

func createAgency(r map[string]string, opts *ParseOptions) (ag *gtfs.Agency, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Agency)

	a.Id = getString("agency_id", r, false)
	a.Name = getString("agency_name", r, true)
	a.Url = getURL("agency_url", r, true, opts.UseDefValueOnError)
	a.Timezone = getTimezone("agency_timezone", r, true, opts.UseDefValueOnError)
	a.Lang = getIsoLangCode("agency_lang", r, false, opts.UseDefValueOnError)
	a.Phone = getString("agency_phone", r, false)
	a.Fare_url = getURL("agency_fare_url", r, false, opts.UseDefValueOnError)
	a.Email = getMail("agency_email", r, false, opts.UseDefValueOnError)

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
	f.Publisher_url = getURL("feed_publisher_url", r, true, opts.UseDefValueOnError)
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

	if !opts.DryRun {
		trip.Frequencies = append(trip.Frequencies, a)
	}

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

	var aID = getString("agency_id", r, false)

	if len(aID) != 0 {
		if val, ok := agencies[aID]; ok {
			a.Agency = val
		} else {
			if opts.UseDefValueOnError {
				a.Agency = nil
			} else {
				return nil, errors.New("No agency with id " + aID + " found.")
			}
		}
	} else if len(agencies) == 1 {
		// if no agency is specified and we only have one agency in agencies.txt, use it here
		for _, ag := range agencies {
			a.Agency = ag
			break
		}
	}

	a.Short_name = getString("route_short_name", r, true)
	a.Long_name = getString("route_long_name", r, true)
	a.Desc = getString("route_desc", r, false)
	a.Type = int16(getRangeInt("route_type", r, true, 0, 1702)) // allow extended route types
	a.Url = getURL("route_url", r, false, opts.UseDefValueOnError)
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
	service.Exceptions = make(map[gtfs.Date]int8, 0)

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
		service.Exceptions = make(map[gtfs.Date]int8, 0)
	}

	// create exception
	t := getRangeInt("exception_type", r, true, 1, 2)
	date := getDate("date", r, true, false)

	// may be nil during dry run
	if service != nil {
		if _, ok := service.Exceptions[date]; ok {
			return nil, errors.New("Date exception for service id " + getString("service_id", r, true) + " defined 2 times for one date.")
		}
		service.Exceptions[date] = int8(t)
	}

	if update {
		return nil, nil
	}
	return service, nil
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
	a.Url = getURL("stop_url", r, false, opts.UseDefValueOnError)
	a.Location_type = getBool("location_type", r, false, false, opts.UseDefValueOnError)
	a.Parent_station = nil
	a.Timezone = getTimezone("stop_timezone", r, false, opts.UseDefValueOnError)
	a.Wheelchair_boarding = int8(getRangeIntWithDefault("wheelchair_boarding", r, 0, 2, 0, opts.UseDefValueOnError))

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

	if a.Stop.Location_type {
		panic(errors.New("Stop " + a.Stop.Id + " (" + a.Stop.Name + ") has location_type=1, cannot be used in stop_times.txt!"))
	}

	a.Arrival_time = getTime("arrival_time", r)
	a.Departure_time = getTime("departure_time", r)

	if a.Arrival_time.Empty() && !a.Departure_time.Empty() {
		if opts.UseDefValueOnError {
			a.Arrival_time = a.Departure_time
		} else {
			panic(errors.New("Missing arrival time for " + getString("stop_id", r, true) + "."))
		}
	}

	if !a.Arrival_time.Empty() && a.Departure_time.Empty() {
		if opts.UseDefValueOnError {
			a.Departure_time = a.Arrival_time
		} else {
			panic(errors.New("Missing departure time for " + getString("stop_id", r, true) + "."))
		}
	}

	if a.Arrival_time.SecondsSinceMidnight() > a.Departure_time.SecondsSinceMidnight() {
		panic(errors.New("Departure before arrival at stop " + getString("stop_id", r, true) + "."))
	}

	a.Sequence = getPositiveInt("stop_sequence", r, true)
	a.Headsign = getString("stop_headsign", r, false)
	a.Pickup_type = int8(getRangeInt("pickup_type", r, false, 0, 3))
	a.Drop_off_type = int8(getRangeInt("drop_off_type", r, false, 0, 3))
	dist, nulled := getNullableFloat("shape_dist_traveled", r, opts.UseDefValueOnError)
	a.Shape_dist_traveled = dist
	a.Has_dist = !nulled
	a.Timepoint = getBool("timepoint", r, false, !a.Arrival_time.Empty() && !a.Departure_time.Empty(), opts.UseDefValueOnError)

	if (a.Arrival_time.Empty() || a.Departure_time.Empty()) && a.Timepoint {
		if opts.UseDefValueOnError {
			a.Timepoint = false
		} else if !opts.DropErroneous {
			panic(errors.New("Stops with timepoint=1 cannot have empty arrival or departure time"))
		}
	}

	if checkStopTimesOrdering(a.Sequence, trip.StopTimes) {
		trip.StopTimes = append(trip.StopTimes, a)
	} else if !opts.DropErroneous {
		panic(errors.New("Stop time sequence collision. Sequence has to increase along trip"))
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
		panic(fmt.Errorf("No route with id %s found", getString("route_id", r, true)))
	}

	if val, ok := services[getString("service_id", r, true)]; ok {
		a.Service = val
	} else {
		panic(fmt.Errorf("No service with id %s found", getString("service_id", r, true)))
	}

	a.Headsign = getString("trip_headsign", r, false)
	a.Short_name = getString("trip_short_name", r, false)
	a.Direction_id = int8(getRangeInt("direction_id", r, false, 0, 1))
	a.Block_id = getString("block_id", r, false)

	shapeID := getString("shape_id", r, false)

	if len(shapeID) > 0 {
		if val, ok := shapes[shapeID]; ok {
			a.Shape = val
		} else {
			if opts.UseDefValueOnError {
				a.Shape = nil
			} else {
				return nil, fmt.Errorf("No shape with id %s found", shapeID)
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

	shapeID := getString("shape_id", r, true)
	var shape *gtfs.Shape

	if val, ok := shapes[shapeID]; ok {
		shape = val
	} else {
		// create new shape
		shape = new(gtfs.Shape)
		shape.Id = shapeID
		// push it onto the shape map
		shapes[shapeID] = shape
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
		panic(errors.New("Shape point sequence collision. Sequence has to increase along shape"))
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
		panic(fmt.Errorf("No fare attribute with id %s found", fareid))
	}

	// create fare attribute
	rule := new(gtfs.FareAttributeRule)

	routeID := getString("route_id", r, false)

	if len(routeID) > 0 {
		if val, ok := routes[routeID]; ok {
			rule.Route = val
		} else {
			panic(fmt.Errorf("No route with id %s found", routeID))
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

	a.Transfer_type = getRangeInt("transfer_type", r, false, 0, 3)
	a.Min_transfer_time = getPositiveIntWithDefault("min_transfer_time", r, -1, opts.UseDefValueOnError)

	return a, nil
}

func getString(name string, r map[string]string, req bool) string {
	if val, ok := r[name]; ok {
		return strings.TrimSpace(val)
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return ""
}

func getURL(name string, r map[string]string, req bool, ignErrs bool) *url.URL {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		u, e := url.ParseRequestURI(strings.TrimSpace(val))
		if e != nil && (req || !ignErrs) {
			panic(fmt.Errorf("'%s' is not a valid url", val))
		} else if e != nil {
			return nil
		}
		return u
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return nil
}

func getMail(name string, r map[string]string, req bool, ignErrs bool) *mail.Address {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		u, e := mail.ParseAddress(strings.TrimSpace(val))
		if e != nil && (req || !ignErrs) {
			panic(fmt.Errorf("'%s' is not a valid email address", val))
		} else if e != nil {
			return nil
		}
		return u
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return nil
}

func getTimezone(name string, r map[string]string, req bool, ignErrs bool) gtfs.Timezone {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		tz, e := gtfs.NewTimezone(strings.TrimSpace(val))
		if e != nil && (req || !ignErrs) {
			panic(e)
		} else if e != nil {
			return tz
		}
		return tz
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	tz, _ := gtfs.NewTimezone("")
	return tz
}

func getIsoLangCode(name string, r map[string]string, req bool, ignErrs bool) gtfs.LanguageISO6391 {
	if val, ok := r[name]; ok && len(strings.TrimSpace(val)) > 0 {
		l, e := gtfs.NewLanguageISO6391(strings.TrimSpace(val))
		if e != nil && (req || !ignErrs) {
			panic(e)
		} else if e != nil {
			return l
		}
		return l
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	l, _ := gtfs.NewLanguageISO6391("")
	return l
}

func getColor(name string, r map[string]string, req bool, def string, ignErrs bool) string {
	if val, ok := r[name]; ok && len(val) > 0 {
		val = strings.TrimSpace(val)
		if len(val) != 6 {
			if ignErrs {
				return def
			}
			panic(fmt.Errorf("Expected six-character hexadecimal number as color for field '%s' (found: %s)", name, val))
		}

		if _, e := hex.DecodeString(val); e != nil {
			if ignErrs {
				return def
			}
			panic(fmt.Errorf("Expected hexadecimal number as color for field '%s' (found: %s)", name, val))
		}
		return strings.ToUpper(val)
	} else if req {
		if ignErrs {
			return def
		}
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return strings.ToUpper(def)
}

func getInt(name string, r map[string]string, req bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil {
			panic(fmt.Errorf("Expected integer for field '%s', found '%s'", name, val))
		}
		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return 0
}

func getPositiveInt(name string, r map[string]string, req bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil || num < 0 {
			panic(fmt.Errorf("Expected positive integer for field '%s', found '%s'", name, val))
		}
		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return 0
}

func getPositiveIntWithDefault(name string, r map[string]string, def int, ignErrs bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil || num < 0 {
			if ignErrs {
				return def
			}
			panic(fmt.Errorf("Expected positive integer for field '%s', found '%s'", name, val))
		}
		return num
	}
	return def
}

func getRangeInt(name string, r map[string]string, req bool, min int, max int) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil {
			panic(fmt.Errorf("Expected integer for field '%s', found '%s'", name, val))
		}

		if num > max || num < min {
			panic(fmt.Errorf("Expected integer between %d and %d for field '%s', found %s", min, max, name, val))
		}

		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return 0
}

func getRangeIntWithDefault(name string, r map[string]string, min int, max int, def int, ignErrs bool) int {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.Atoi(val)
		if err != nil {
			if ignErrs {
				return def
			}
			panic(fmt.Errorf("Expected integer for field '%s', found '%s'", name, val))
		}

		if num > max || num < min {
			if ignErrs {
				return def
			}
			panic(fmt.Errorf("Expected integer between %d and %d for field '%s', found %s", min, max, name, val))
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
			panic(fmt.Errorf("Expected float for field '%s', found '%s'", name, val))
		}
		return float32(num)
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return -1
}

func getNullableFloat(name string, r map[string]string, ignErrs bool) (float32, bool) {
	if val, ok := r[name]; ok && len(val) > 0 {
		num, err := strconv.ParseFloat(strings.TrimSpace(val), 32)
		if err != nil {
			if ignErrs {
				return 0, true
			}
			panic(fmt.Errorf("Expected float for field '%s', found '%s'", name, val))
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
			}
			panic(fmt.Errorf("Expected 1 or 0 for field '%s', found '%s'", name, val))
		}
		return num == 1
	} else if req {
		if ignErrs {
			return def
		}
		panic(fmt.Errorf("Expected required field '%s'", name))
	}
	return def
}

func getDate(name string, r map[string]string, req bool, ignErrs bool) gtfs.Date {
	var str string
	var ok bool
	if str, ok = r[name]; !ok || len(str) == 0 {
		if req {
			panic(fmt.Errorf("Expected required field '%s'", name))
		} else {
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

	if e != nil && !ignErrs {
		panic(fmt.Errorf("Expected YYYYMMDD date for field '%s', found '%s' (%s)", name, str, e.Error()))
	} else {
		return gtfs.Date{Day: int8(day), Month: int8(month), Year: int16(year)}
	}
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
		e = fmt.Errorf("expected to be in HH:MM:SS format: '%s'", str)
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
		panic(fmt.Errorf("Expected HH:MM:SS time for field '%s', found '%s' (%s)", name, str, e.Error()))
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
