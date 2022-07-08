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
	"regexp"
	"strconv"
	"strings"
)

// csv lookup structs
type Fields interface {
	FldName(idx int) string
}

func addiFields(header []string, flds Fields) []int {
	a := make([]int, 0)

	for i, _ := range header {
		if len(flds.FldName(i)) == 0 {
			a = append(a, i)
		}
	}

	return a
}

type TranslationFields struct {
	tableName   int
	fieldName   int
	language    int
	translation int
	recordId    int
	recordSubId int
	fieldValue  int
}

func (flds TranslationFields) FldName(idx int) (name string) {
	switch idx {
	case flds.tableName:
		return "table_name"
	case flds.fieldName:
		return "field_name"
	case flds.language:
		return "language"
	case flds.translation:
		return "translation"
	case flds.recordId:
		return "record_id"
	case flds.recordSubId:
		return "record_sub_id"
	case flds.fieldValue:
		return "field_value"
	default:
		return ""
	}
}

type AttributionFields struct {
	attributionId    int
	organizationName int
	isProducer       int
	isOperator       int
	isAuthority      int
	attributionUrl   int
	attributionEmail int
	attributionPhone int
	routeId          int
	agencyId         int
	tripId           int
}

func (flds AttributionFields) FldName(idx int) (name string) {
	switch idx {
	case flds.attributionId:
		return "attribution_id"
	case flds.organizationName:
		return "organization_name"
	case flds.isProducer:
		return "is_producer"
	case flds.isOperator:
		return "is_operator"
	case flds.isAuthority:
		return "is_authority"
	case flds.attributionUrl:
		return "attribution_url"
	case flds.attributionEmail:
		return "attribution_email"
	case flds.attributionPhone:
		return "attribution_phone"
	case flds.routeId:
		return "route_id"
	case flds.agencyId:
		return "agency_id"
	case flds.tripId:
		return "trip_id"
	default:
		return ""
	}
}

type LevelFields struct {
	levelId    int
	levelIndex int
	levelName  int
}

func (flds LevelFields) FldName(idx int) (name string) {
	switch idx {
	case flds.levelId:
		return "level_id"
	case flds.levelIndex:
		return "level_index"
	case flds.levelName:
		return "level_name"
	default:
		return ""
	}
}

type ShapeFields struct {
	shapeId           int
	shapeDistTraveled int
	shapePtLat        int
	shapePtLon        int
	shapePtSequence   int
}

func (flds ShapeFields) FldName(idx int) (name string) {
	switch idx {
	case flds.shapeId:
		return "shape_id"
	case flds.shapeDistTraveled:
		return "shape_dist_traveled"
	case flds.shapePtLat:
		return "shape_pt_lat"
	case flds.shapePtLon:
		return "shape_pt_lon"
	case flds.shapePtSequence:
		return "shape_pt_sequence"
	default:
		return ""
	}
}

type FeedInfoFields struct {
	feedPublisherName int
	feedPublisherUrl  int
	feedLang          int
	feedStartDate     int
	feedEndDate       int
	feedVersion       int
	feedContactEmail  int
	feedContactUrl    int
}

func (flds FeedInfoFields) FldName(idx int) (name string) {
	switch idx {
	case flds.feedPublisherName:
		return "feed_publisher_name"
	case flds.feedPublisherUrl:
		return "feed_publisher_url"
	case flds.feedLang:
		return "feed_lang"
	case flds.feedStartDate:
		return "feed_start_date"
	case flds.feedEndDate:
		return "feed_end_date"
	case flds.feedVersion:
		return "feed_version"
	case flds.feedContactEmail:
		return "feed_contact_email"
	case flds.feedContactUrl:
		return "feed_contact_url"
	default:
		return ""
	}
}

type TransferFields struct {
	FromStopId      int
	ToStopId        int
	TransferType    int
	MinTransferTime int
}

func (flds TransferFields) FldName(idx int) (name string) {
	switch idx {
	case flds.FromStopId:
		return "from_stop_id"
	case flds.ToStopId:
		return "to_stop_id"
	case flds.TransferType:
		return "transfer_type"
	case flds.MinTransferTime:
		return "min_transfer_time"
	default:
		return ""
	}
}

type CalendarFields struct {
	serviceId int
	monday    int
	tuesday   int
	wednesday int
	thursday  int
	friday    int
	saturday  int
	sunday    int
	startDate int
	endDate   int
}

func (flds CalendarFields) FldName(idx int) (name string) {
	switch idx {
	case flds.serviceId:
		return "service_id"
	case flds.monday:
		return "monday"
	case flds.tuesday:
		return "tuesday"
	case flds.wednesday:
		return "wednesday"
	case flds.thursday:
		return "thursday"
	case flds.friday:
		return "friday"
	case flds.saturday:
		return "saturday"
	case flds.sunday:
		return "sunday"
	case flds.startDate:
		return "start_date"
	case flds.endDate:
		return "end_date"
	default:
		return ""
	}
}

type FrequencyFields struct {
	tripId      int
	exactTimes  int
	startTime   int
	endTime     int
	headwaySecs int
}

func (flds FrequencyFields) FldName(idx int) (name string) {
	switch idx {
	case flds.tripId:
		return "trip_id"
	case flds.exactTimes:
		return "exact_times"
	case flds.startTime:
		return "start_time"
	case flds.endTime:
		return "end_time"
	case flds.headwaySecs:
		return "headway_secs"
	default:
		return ""
	}
}

type CalendarDatesFields struct {
	serviceId     int
	exceptionType int
	date          int
}

func (flds CalendarDatesFields) FldName(idx int) (name string) {
	switch idx {
	case flds.serviceId:
		return "service_id"
	case flds.exceptionType:
		return "exception_type"
	case flds.date:
		return "date"
	default:
		return ""
	}
}

type StopFields struct {
	stopId             int
	stopCode           int
	locationType       int
	stopName           int
	stopDesc           int
	stopLat            int
	stopLon            int
	zoneId             int
	stopUrl            int
	parentStation      int
	stopTimezone       int
	levelId            int
	platformCode       int
	wheelchairBoarding int
}

func (flds StopFields) FldName(idx int) (name string) {
	switch idx {
	case flds.stopId:
		return "stop_id"
	case flds.stopCode:
		return "stop_code"
	case flds.locationType:
		return "location_type"
	case flds.stopName:
		return "stop_name"
	case flds.stopDesc:
		return "stop_desc"
	case flds.stopLat:
		return "stop_lat"
	case flds.stopLon:
		return "stop_lon"
	case flds.zoneId:
		return "zone_id"
	case flds.stopUrl:
		return "stop_url"
	case flds.parentStation:
		return "parent_station"
	case flds.stopTimezone:
		return "stop_timezone"
	case flds.levelId:
		return "level_id"
	case flds.platformCode:
		return "platform_code"
	case flds.wheelchairBoarding:
		return "wheelchair_boarding"
	default:
		return ""
	}
}

type StopTimeFields struct {
	tripId            int
	stopId            int
	arrivalTime       int
	departureTime     int
	stopSequence      int
	stopHeadsign      int
	pickupType        int
	dropOffType       int
	continuousDropOff int
	continuousPickup  int
	shapeDistTraveled int
	timepoint         int
}

func (flds StopTimeFields) FldName(idx int) (name string) {
	switch idx {
	case flds.tripId:
		return "trip_id"
	case flds.stopId:
		return "stop_id"
	case flds.arrivalTime:
		return "arrival_time"
	case flds.departureTime:
		return "departure_time"
	case flds.stopSequence:
		return "stop_sequence"
	case flds.stopHeadsign:
		return "stop_headsign"
	case flds.pickupType:
		return "pickup_type"
	case flds.dropOffType:
		return "drop_off_type"
	case flds.continuousDropOff:
		return "continuous_drop_off"
	case flds.continuousPickup:
		return "continuous_pickup"
	case flds.shapeDistTraveled:
		return "shape_dist_traveled"
	case flds.timepoint:
		return "timepoint"
	default:
		return ""
	}
}

type FareAttributeFields struct {
	fareId           int
	price            int
	currencyType     int
	paymentMethod    int
	transfers        int
	transferDuration int
	agencyId         int
}

func (flds FareAttributeFields) FldName(idx int) (name string) {
	switch idx {
	case flds.fareId:
		return "fare_id"
	case flds.price:
		return "price"
	case flds.currencyType:
		return "currency_type"
	case flds.paymentMethod:
		return "payment_method"
	case flds.transfers:
		return "transfers"
	case flds.transferDuration:
		return "transfer_duration"
	case flds.agencyId:
		return "agency_id"
	default:
		return ""
	}
}

type FareRuleFields struct {
	fareId        int
	routeId       int
	originId      int
	destinationId int
	containsId    int
}

func (flds FareRuleFields) FldName(idx int) (name string) {
	switch idx {
	case flds.fareId:
		return "fare_id"
	case flds.routeId:
		return "route_id"
	case flds.originId:
		return "origin_id"
	case flds.destinationId:
		return "destination_id"
	case flds.containsId:
		return "contains_id"
	default:
		return ""
	}
}

type RouteFields struct {
	routeId           int
	agencyId          int
	routeShortName    int
	routeLongName     int
	routeDesc         int
	routeType         int
	routeUrl          int
	routeColor        int
	routeTextColor    int
	routeSortOrder    int
	continuousDropOff int
	continuousPickup  int
}

func (flds RouteFields) FldName(idx int) (name string) {
	switch idx {
	case flds.routeId:
		return "route_id"
	case flds.agencyId:
		return "agency_id"
	case flds.routeShortName:
		return "route_short_name"
	case flds.routeLongName:
		return "route_long_name"
	case flds.routeDesc:
		return "route_desc"
	case flds.routeType:
		return "route_type"
	case flds.routeUrl:
		return "route_url"
	case flds.routeColor:
		return "route_color"
	case flds.routeTextColor:
		return "route_text_color"
	case flds.routeSortOrder:
		return "route_sort_color"
	case flds.continuousDropOff:
		return "continuous_drop_off"
	case flds.continuousPickup:
		return "continuous_pickup"
	default:
		return ""
	}
}

type TripFields struct {
	tripId               int
	routeId              int
	serviceId            int
	tripHeadsign         int
	tripShortName        int
	directionId          int
	blockId              int
	shapeId              int
	wheelchairAccessible int
	bikesAllowed         int
}

func (flds TripFields) FldName(idx int) (name string) {
	switch idx {
	case flds.tripId:
		return "trip_id"
	case flds.routeId:
		return "route_id"
	case flds.serviceId:
		return "service_id"
	case flds.tripHeadsign:
		return "trip_headsign"
	case flds.tripShortName:
		return "trip_short_name"
	case flds.directionId:
		return "direction_id"
	case flds.blockId:
		return "block_id"
	case flds.shapeId:
		return "shape_id"
	case flds.wheelchairAccessible:
		return "wheelchair_accessible"
	case flds.bikesAllowed:
		return "bikes_allowed"
	default:
		return ""
	}
}

type AgencyFields struct {
	agencyId       int
	agencyName     int
	agencyUrl      int
	agencyTimezone int
	agencyLang     int
	agencyPhone    int
	agencyFareUrl  int
	agencyEmail    int
}

func (flds AgencyFields) FldName(idx int) (name string) {
	switch idx {
	case flds.agencyId:
		return "agency_id"
	case flds.agencyName:
		return "agency_name"
	case flds.agencyUrl:
		return "agency_url"
	case flds.agencyTimezone:
		return "agency_timezone"
	case flds.agencyLang:
		return "agency_lang"
	case flds.agencyPhone:
		return "agency_phone"
	case flds.agencyFareUrl:
		return "agency_fare_url"
	case flds.agencyEmail:
		return "agency_email"
	default:
		return ""
	}
}

type PathwayFields struct {
	pathwayId            int
	fromStopId           int
	toStopId             int
	pathwayMode          int
	isBidirectional      int
	length               int
	traversalTime        int
	stairCount           int
	maxSlope             int
	minWidth             int
	signpostedAs         int
	reversedSignpostedAs int
}

func (flds PathwayFields) FldName(idx int) (name string) {
	switch idx {
	case flds.pathwayId:
		return "pathway_id"
	case flds.fromStopId:
		return "from_stop_id"
	case flds.toStopId:
		return "to_stop_id"
	case flds.pathwayMode:
		return "pathway_mode"
	case flds.isBidirectional:
		return "is_bidirectional"
	case flds.length:
		return "length"
	case flds.traversalTime:
		return "traversal_time"
	case flds.stairCount:
		return "stair_count"
	case flds.maxSlope:
		return "max_slope"
	case flds.minWidth:
		return "min_width"
	case flds.signpostedAs:
		return "signposted_as"
	case flds.reversedSignpostedAs:
		return "reversed_signposted_as"
	default:
		return ""
	}
}

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

func createTranslation(r []string, flds TranslationFields, feed *Feed, prefix string) (attr *gtfs.Translation, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	tr := new(gtfs.Translation)
	tr.FieldName = getString(flds.fieldName, r, flds, true, true, "")
	tr.Translation = getString(flds.translation, r, flds, true, true, "")
	tr.FieldValue = getString(flds.fieldValue, r, flds, false, false, "")
	tr.Language = getIsoLangCode(flds.language, r, flds, false, false, feed)

	tableName := getString(flds.tableName, r, flds, true, true, "")

	if !feed.opts.DryRun && !(tableName == "agency" || tableName == "stops" || tableName == "routes" || tableName == "trips" || tableName == "stop_times" || tableName == "feed_info" || tableName == "pathways" || tableName == "attributions" || tableName == "levels") {
		panic(fmt.Errorf("table_name must be one of: 'agency', 'stops', 'routes', 'trips', 'stop_times', 'feed_info', 'pathways', 'attributions', 'levels' (found '%s')", tableName))
	}

	strings.Replace(strings.ToLower(tableName), ".txt", "", 1)

	id := getString(flds.recordId, r, flds, false, false, "")
	// subId := getString(flds.recordSubId, r, flds, false, false, "")

	if len(id) > 0 {
		if tableName == "agency" {
			if ag, ok := feed.Agencies[prefix+id]; ok {
				ag.Translations = append(ag.Translations, tr)
			} else {
				panic(fmt.Errorf("No agency with id %s found", id))
			}
		} else if tableName == "stops" {
			if st, ok := feed.Stops[prefix+id]; ok {
				st.Translations = append(st.Translations, tr)
			} else {
				panic(fmt.Errorf("No stop with id %s found", id))
			}
		} else if tableName == "trips" {
			if trip, ok := feed.Trips[prefix+id]; ok {
				trip.Translations = append(trip.Translations, tr)
			} else {
				panic(fmt.Errorf("No trip with id %s found", id))
			}
		} else if tableName == "feed_info" {
			panic(fmt.Errorf("Cannot use record_id for table_name 'feed_info'"))
		} else if tableName == "pathways" {
			if pw, ok := feed.Pathways[prefix+id]; ok {
				pw.Translations = append(pw.Translations, tr)
			} else {
				panic(fmt.Errorf("No pathway with id %s found", id))
			}
		} else if tableName == "levels" {
			if lvl, ok := feed.Levels[prefix+id]; ok {
				lvl.Translations = append(lvl.Translations, tr)
			} else {
				panic(fmt.Errorf("No level with id %s found", id))
			}
		}
	}

	return tr, nil
}

func createAttribution(r []string, flds AttributionFields, feed *Feed, prefix string) (attr *gtfs.Attribution, ag *gtfs.Agency, route *gtfs.Route, trip *gtfs.Trip, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Attribution)

	a.Id = prefix + getString(flds.attributionId, r, flds, false, false, "")
	a.Organization_name = getString(flds.organizationName, r, flds, true, true, feed.opts.EmptyStringRepl)
	a.Is_producer = getBool(flds.isProducer, r, flds, false, false, feed.opts.UseDefValueOnError, feed)
	a.Is_operator = getBool(flds.isOperator, r, flds, false, false, feed.opts.UseDefValueOnError, feed)
	a.Is_authority = getBool(flds.isAuthority, r, flds, false, false, feed.opts.UseDefValueOnError, feed)

	a.Url = getURL(flds.attributionUrl, r, flds, false, feed.opts.UseDefValueOnError, feed)
	a.Email = getMail(flds.attributionEmail, r, flds, false, feed.opts.UseDefValueOnError, feed)
	a.Phone = getString(flds.attributionPhone, r, flds, false, false, feed.opts.EmptyStringRepl)

	routeId := getString(flds.routeId, r, flds, false, false, "")
	agencyId := getString(flds.agencyId, r, flds, false, false, "")
	tripId := getString(flds.tripId, r, flds, false, false, "")

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

func createAgency(r []string, flds AgencyFields, feed *Feed, prefix string) (ag *gtfs.Agency, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Agency)

	a.Id = prefix + getString(flds.agencyId, r, flds, false, false, "")
	a.Name = getString(flds.agencyName, r, flds, true, true, feed.opts.EmptyStringRepl)
	a.Url = getURL(flds.agencyUrl, r, flds, true, feed.opts.UseDefValueOnError, feed)
	a.Timezone = getTimezone(flds.agencyTimezone, r, flds, true, feed.opts.UseDefValueOnError, feed)
	a.Lang = getIsoLangCode(flds.agencyLang, r, flds, false, feed.opts.UseDefValueOnError, feed)
	a.Phone = getString(flds.agencyPhone, r, flds, false, false, "")
	a.Fare_url = getURL(flds.agencyFareUrl, r, flds, false, feed.opts.UseDefValueOnError, feed)
	a.Email = getMail(flds.agencyEmail, r, flds, false, feed.opts.UseDefValueOnError, feed)

	return a, nil
}

func createFeedInfo(r []string, flds FeedInfoFields, feed *Feed) (fi *gtfs.FeedInfo, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	f := new(gtfs.FeedInfo)

	f.Publisher_name = getString(flds.feedPublisherName, r, flds, true, true, feed.opts.EmptyStringRepl)
	f.Publisher_url = getURL(flds.feedPublisherUrl, r, flds, true, feed.opts.UseDefValueOnError, feed)
	f.Lang = getString(flds.feedLang, r, flds, true, true, feed.opts.EmptyStringRepl)
	f.Start_date = getDate(flds.feedStartDate, r, flds, false, feed.opts.UseDefValueOnError, feed)
	f.End_date = getDate(flds.feedEndDate, r, flds, false, feed.opts.UseDefValueOnError, feed)
	f.Version = getString(flds.feedVersion, r, flds, false, false, "")
	f.Contact_email = getMail(flds.feedContactEmail, r, flds, false, feed.opts.UseDefValueOnError, feed)
	f.Contact_url = getURL(flds.feedContactUrl, r, flds, false, feed.opts.UseDefValueOnError, feed)

	return f, nil
}

func createFrequency(r []string, flds FrequencyFields, feed *Feed, prefix string) (tr *gtfs.Trip, freq *gtfs.Frequency, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Frequency)
	var trip *gtfs.Trip

	tripid := prefix + getString(flds.tripId, r, flds, true, true, "")

	if val, ok := feed.Trips[tripid]; ok {
		trip = val
	} else {
		panic(errors.New("No trip with id " + r[flds.tripId] + " found."))
	}

	a.Exact_times = getBool(flds.exactTimes, r, flds, false, false, feed.opts.UseDefValueOnError, feed)
	a.Start_time = getTime(flds.startTime, r, flds)
	a.End_time = getTime(flds.endTime, r, flds)

	if a.Start_time.SecondsSinceMidnight() > a.End_time.SecondsSinceMidnight() {
		panic(errors.New("Frequency has start_time > end_time."))
	}

	a.Headway_secs = getPositiveInt(flds.headwaySecs, r, flds, true)

	if !feed.opts.DryRun {
		trip.Frequencies = append(trip.Frequencies, a)
	}

	return trip, a, nil
}

func createRoute(r []string, flds RouteFields, feed *Feed, prefix string) (route *gtfs.Route, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Route)
	a.Id = prefix + getString(flds.routeId, r, flds, true, true, "")

	var aID = prefix + getString(flds.agencyId, r, flds, false, false, "")

	if len(aID) != len(prefix) {
		if val, ok := feed.Agencies[aID]; ok {
			a.Agency = val
		} else {
			if feed.opts.UseDefValueOnError {
				if len(feed.Agencies) == 1 {
					a.Agency = nil
				} else {
					return nil, errors.New("Agency with id " + getString(flds.agencyId, r, flds, false, false, "") + " not found or erroneous, cannot fall back to no agency as there is more than one agency in agency.txt.")
				}
			} else {
				return nil, errors.New("No agency with id " + getString(flds.agencyId, r, flds, false, false, "") + " found.")
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

	a.Short_name = getString(flds.routeShortName, r, flds, false, false, "")
	a.Long_name = getString(flds.routeLongName, r, flds, false, false, "")

	if len(a.Short_name) == 0 && len(a.Long_name) == 0 {
		return nil, errors.New("Either route_short_name or route_long_name are required.")
	}

	a.Desc = getString(flds.routeDesc, r, flds, false, false, "")
	a.Type = int16(getRangeInt(flds.routeType, r, flds, true, 0, 1702)) // allow extended route types
	a.Url = getURL(flds.routeUrl, r, flds, false, feed.opts.UseDefValueOnError, feed)
	a.Color = getColor(flds.routeColor, r, flds, false, "ffffff", feed.opts.UseDefValueOnError, feed)
	a.Text_color = getColor(flds.routeTextColor, r, flds, false, "000000", feed.opts.UseDefValueOnError, feed)
	a.Sort_order = getPositiveIntWithDefault(flds.routeSortOrder, r, flds, -1, feed.opts.UseDefValueOnError, feed)
	a.Continuous_pickup = int8(getRangeIntWithDefault(flds.continuousPickup, r, flds, 0, 3, 1, feed.opts.UseDefValueOnError, feed))
	a.Continuous_drop_off = int8(getRangeIntWithDefault(flds.continuousDropOff, r, flds, 0, 3, 1, feed.opts.UseDefValueOnError, feed))

	return a, nil
}

func createServiceFromCalendar(r []string, flds CalendarFields, feed *Feed, prefix string) (s *gtfs.Service, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	service := new(gtfs.Service)
	service.Id = prefix + getString(flds.serviceId, r, flds, true, true, "")
	service.Exceptions = make(map[gtfs.Date]bool, 0)

	// fill daybitmap
	service.Daymap[1] = getBool(flds.monday, r, flds, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[2] = getBool(flds.tuesday, r, flds, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[3] = getBool(flds.wednesday, r, flds, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[4] = getBool(flds.thursday, r, flds, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[5] = getBool(flds.friday, r, flds, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[6] = getBool(flds.saturday, r, flds, true, false, feed.opts.UseDefValueOnError, feed)
	service.Daymap[0] = getBool(flds.sunday, r, flds, true, false, feed.opts.UseDefValueOnError, feed)
	service.Start_date = getDate(flds.startDate, r, flds, true, false, feed)
	service.End_date = getDate(flds.endDate, r, flds, true, false, feed)

	if service.End_date.GetTime().Before(service.Start_date.GetTime()) {
		return nil, errors.New("Service " + getString(flds.serviceId, r, flds, true, true, "") + " has the end date before the start date.")
	}

	return service, nil
}

func createServiceFromCalendarDates(r []string, flds CalendarDatesFields, feed *Feed, filterDateStart gtfs.Date, filterDateEnd gtfs.Date, prefix string) (s *gtfs.Service, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	update := false
	var service *gtfs.Service

	// first, check if the service already exists
	if val, ok := feed.Services[prefix+getString(flds.serviceId, r, flds, true, true, "")]; ok {
		service = val
		update = true
	} else {
		service = new(gtfs.Service)
		service.Id = prefix + getString(flds.serviceId, r, flds, true, true, "")
		service.Exceptions = make(map[gtfs.Date]bool, 0)
	}

	// create exception
	t := getRangeInt(flds.exceptionType, r, flds, true, 1, 2)
	date := getDate(flds.date, r, flds, true, false, feed)

	// may be nil during dry run
	if service != nil {
		if _, ok := service.Exceptions[date]; ok {
			return nil, errors.New("Date exception for service id " + getString(flds.serviceId, r, flds, true, true, "") + " defined 2 times for one date.")
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

func createStop(r []string, flds StopFields, feed *Feed, prefix string) (s *gtfs.Stop, pid string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Stop)
	parentId := ""

	a.Id = prefix + getString(flds.stopId, r, flds, true, true, "")
	a.Code = getString(flds.stopCode, r, flds, false, false, "")
	a.Location_type = int8(getRangeIntWithDefault(flds.locationType, r, flds, 0, 4, 0, feed.opts.UseDefValueOnError, feed))
	a.Name = getString(flds.stopName, r, flds, a.Location_type < 3, a.Location_type < 3, feed.opts.EmptyStringRepl)
	a.Desc = getString(flds.stopDesc, r, flds, false, false, "")

	if a.Location_type < 3 {
		a.Lat = getFloat(flds.stopLat, r, flds, true)
		a.Lon = getFloat(flds.stopLon, r, flds, true)
	} else {
		lat := getNullableFloat(flds.stopLat, r, flds, feed.opts.UseDefValueOnError, feed)
		lon := getNullableFloat(flds.stopLon, r, flds, feed.opts.UseDefValueOnError, feed)

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

	a.Zone_id = prefix + getString(flds.zoneId, r, flds, false, false, "")
	if len(a.Zone_id) == len(prefix) {
		a.Zone_id = ""
	}
	a.Url = getURL(flds.stopUrl, r, flds, false, feed.opts.UseDefValueOnError, feed)

	// will be filled later on
	a.Parent_station = nil

	if a.Location_type > 1 {
		parentId = prefix + getString(flds.parentStation, r, flds, true, true, "")
	} else if a.Location_type == 0 {
		parentId = prefix + getString(flds.parentStation, r, flds, false, false, "")
	} else {
		if len(getString(flds.parentStation, r, flds, false, false, "")) > 0 {
			panic(fmt.Errorf("'parent_station' cannot be defined for location_type=1."))
		}
	}

	a.Timezone = getTimezone(flds.stopTimezone, r, flds, false, feed.opts.UseDefValueOnError, feed)
	a.Wheelchair_boarding = int8(getRangeIntWithDefault(flds.wheelchairBoarding, r, flds, 0, 2, 0, feed.opts.UseDefValueOnError, feed))
	a.Level = nil

	levelId := prefix + getString(flds.levelId, r, flds, false, false, "")

	if len(levelId) > len(prefix) {
		if val, ok := feed.Levels[levelId]; ok {
			a.Level = val
		} else {
			panic(errors.New("No level with id " + getString(flds.levelId, r, flds, false, true, "") + " found."))
		}
	}

	a.Platform_code = getString(flds.platformCode, r, flds, false, false, "")

	return a, parentId, nil
}

func reserveStopTime(r []string, flds StopTimeFields, feed *Feed, prefix string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	var trip *gtfs.Trip

	if val, ok := feed.Trips[prefix+getString(flds.tripId, r, flds, true, true, "")]; ok {
		trip = val
	} else {
		panic(errors.New("No trip with id " + getString(flds.tripId, r, flds, true, true, "") + " found."))
	}

	trip.StopTimes[0].Sequence = trip.StopTimes[0].Sequence + 1

	return nil
}

func createStopTime(r []string, flds StopTimeFields, feed *Feed, prefix string) (t *gtfs.Trip, st *gtfs.StopTime, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := gtfs.StopTime{}
	a.Headsign = &feed.emptyString
	var trip *gtfs.Trip

	tripId := prefix + getString(flds.tripId, r, flds, true, true, "")

	if val, ok := feed.Trips[tripId]; ok {
		trip = val
	} else {
		panic(errors.New("No trip with id " + getString(flds.tripId, r, flds, true, true, "") + " found."))
	}

	if trip.Id != tripId {
		trip.Id = tripId
		trip.StopTimes = make(gtfs.StopTimes, 0, trip.StopTimes[0].Sequence)
	}

	if val, ok := feed.Stops[prefix+getString(flds.stopId, r, flds, true, true, "")]; ok {
		a.Stop = val
	} else {
		panic(&StopNotFoundErr{prefix, getString(flds.stopId, r, flds, true, true, "")})
	}

	if a.Stop.Location_type != 0 {
		panic(errors.New("Stop " + a.Stop.Id + " (" + a.Stop.Name + ") has location_type != 0, cannot be used in stop_times.txt!"))
	}

	a.Arrival_time = getTime(flds.arrivalTime, r, flds)
	a.Departure_time = getTime(flds.departureTime, r, flds)

	if a.Arrival_time.Empty() && !a.Departure_time.Empty() {
		if feed.opts.UseDefValueOnError {
			a.Arrival_time = a.Departure_time
		} else {
			panic(errors.New("Missing arrival time for " + getString(flds.stopId, r, flds, true, true, "") + "."))
		}
	}

	if !a.Arrival_time.Empty() && a.Departure_time.Empty() {
		if feed.opts.UseDefValueOnError {
			a.Departure_time = a.Arrival_time
		} else {
			panic(errors.New("Missing departure time for " + getString(flds.stopId, r, flds, true, true, "") + "."))
		}
	}

	if a.Arrival_time.SecondsSinceMidnight() > a.Departure_time.SecondsSinceMidnight() {
		panic(errors.New("Departure before arrival at stop " + getString(flds.stopId, r, flds, true, true, "") + "."))
	}

	a.Sequence = getPositiveInt(flds.stopSequence, r, flds, true)
	headsign := getString(flds.stopHeadsign, r, flds, false, false, "")

	// only store headsigns that are different to the default trip headsign
	if len(headsign) > 0 && headsign != *trip.Headsign {
		if *feed.lastString != headsign {
			feed.lastString = &headsign
		}
		a.Headsign = feed.lastString
	}

	a.Pickup_type = int8(getRangeInt(flds.pickupType, r, flds, false, 0, 3))
	a.Drop_off_type = int8(getRangeInt(flds.dropOffType, r, flds, false, 0, 3))
	a.Continuous_pickup = int8(getRangeIntWithDefault(flds.continuousPickup, r, flds, 0, 3, 1, feed.opts.UseDefValueOnError, feed))
	a.Continuous_drop_off = int8(getRangeIntWithDefault(flds.continuousDropOff, r, flds, 0, 3, 1, feed.opts.UseDefValueOnError, feed))
	dist := getNullableFloat(flds.shapeDistTraveled, r, flds, feed.opts.UseDefValueOnError, feed)
	a.Shape_dist_traveled = dist
	a.Timepoint = getBool(flds.timepoint, r, flds, false, !a.Arrival_time.Empty() && !a.Departure_time.Empty(), feed.opts.UseDefValueOnError, feed)

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

	return trip, &a, nil
}

func createTrip(r []string, flds TripFields, feed *Feed, prefix string) (t *gtfs.Trip, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	a := new(gtfs.Trip)
	a.Id = prefix + getString(flds.tripId, r, flds, true, true, "")

	if val, ok := feed.Routes[prefix+getString(flds.routeId, r, flds, true, true, "")]; ok {
		a.Route = val
	} else {
		panic(fmt.Errorf("No route with id %s found", getString(flds.routeId, r, flds, true, true, "")))
	}

	if val, ok := feed.Services[prefix+getString(flds.serviceId, r, flds, true, true, "")]; ok {
		a.Service = val
	} else {
		panic(fmt.Errorf("No service with id %s found", getString(flds.serviceId, r, flds, true, true, "")))
	}

	headsign := getString(flds.tripHeadsign, r, flds, false, false, "")

	a.Headsign = &feed.emptyString

	if len(headsign) > 0 {
		if *feed.lastString != headsign {
			feed.lastString = &headsign
		}
		a.Headsign = feed.lastString
	}

	a.Short_name = getString(flds.tripShortName, r, flds, false, false, "")
	a.Direction_id = int8(getRangeInt(flds.directionId, r, flds, false, 0, 1))
	a.Block_id = prefix + getString(flds.blockId, r, flds, false, false, "")
	if len(a.Block_id) == len(prefix) {
		a.Block_id = ""
	}

	if !feed.opts.DropShapes {
		shapeID := prefix + getString(flds.shapeId, r, flds, false, false, "")

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

	a.Wheelchair_accessible = int8(getRangeIntWithDefault(flds.wheelchairAccessible, r, flds, 0, 2, 0, feed.opts.UseDefValueOnError, feed))
	a.Bikes_allowed = int8(getRangeIntWithDefault(flds.bikesAllowed, r, flds, 0, 2, 0, feed.opts.UseDefValueOnError, feed))

	return a, nil
}

func reserveShapePoint(r []string, flds ShapeFields, feed *Feed, prefix string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	shapeID := prefix + getString(flds.shapeId, r, flds, true, true, "")
	var shape *gtfs.Shape

	if val, ok := feed.Shapes[shapeID]; ok {
		shape = val
		shape.Points[0].Sequence = shape.Points[0].Sequence + 1
	} else {
		// create new shape
		shape = new(gtfs.Shape)
		shape.Points = append(shape.Points, gtfs.ShapePoint{0, 0, 1, 0})

		// push it onto the shape map
		feed.Shapes[shapeID] = shape
	}

	return nil
}

func createShapePoint(r []string, flds ShapeFields, feed *Feed, prefix string) (s *gtfs.Shape, sp *gtfs.ShapePoint, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	shapeID := prefix + getString(flds.shapeId, r, flds, true, true, "")
	var shape *gtfs.Shape

	if val, ok := feed.Shapes[shapeID]; ok {
		shape = val

		if shape.Id != shapeID {
			shape.Id = shapeID
			shape.Points = make(gtfs.ShapePoints, 0, shape.Points[0].Sequence)
		}
	}

	dist := getNullableFloat(flds.shapeDistTraveled, r, flds, feed.opts.UseDefValueOnError, feed)

	lat := getFloat(flds.shapePtLat, r, flds, true)
	lon := getFloat(flds.shapePtLon, r, flds, true)

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
		Sequence:      getPositiveInt(flds.shapePtSequence, r, flds, true),
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

	return shape, &p, nil
}

func createFareAttribute(r []string, flds FareAttributeFields, feed *Feed, prefix string) (fa *gtfs.FareAttribute, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.FareAttribute)

	a.Id = prefix + getString(flds.fareId, r, flds, true, true, "")
	a.Price = getString(flds.price, r, flds, false, false, "")
	if feed.opts.UseDefValueOnError {
		a.Currency_type = getString(flds.currencyType, r, flds, true, true, "XXX")
	} else {
		a.Currency_type = getString(flds.currencyType, r, flds, true, true, "")
	}
	a.Payment_method = getRangeInt(flds.paymentMethod, r, flds, false, 0, 1)
	a.Transfers = getRangeIntWithDefault(flds.transfers, r, flds, 0, 2, -1, feed.opts.UseDefValueOnError, feed)
	a.Transfer_duration = getPositiveInt(flds.transferDuration, r, flds, false)

	aID := prefix + getString(flds.agencyId, r, flds, false, false, "")

	if len(aID) != len(prefix) {
		if val, ok := feed.Agencies[aID]; ok {
			a.Agency = val
		} else {
			if feed.opts.UseDefValueOnError {
				a.Agency = nil
			} else {
				return nil, errors.New("No agency with id " + getString(flds.agencyId, r, flds, false, false, "") + " found.")
			}
		}
	} else {
		if len(prefix) > 0 {
			prefixCount := 0
			foundId := ""
			for i, _ := range feed.Agencies {
				if strings.HasPrefix(i, prefix) {
					prefixCount = prefixCount + 1
					foundId = i
				}
			}
			if prefixCount > 1 {
				return nil, errors.New("Expected a non-empty value for 'agency_id', as there are multiple agencies defined in agency.txt.")
			} else if prefixCount == 1 {
				a.Agency = feed.Agencies[foundId]
			}
		} else {
			if len(feed.Agencies) > 1 {
				return nil, errors.New("Expected a non-empty value for 'agency_id', as there are multiple agencies defined in agency.txt.")
			}
		}
	}

	return a, nil
}

func createFareRule(r []string, flds FareRuleFields, feed *Feed, prefix string) (fare *gtfs.FareAttribute, rl *gtfs.FareAttributeRule, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	var fareattr *gtfs.FareAttribute
	var fareid string

	fareid = prefix + getString(flds.fareId, r, flds, true, true, "")

	// first, check if the service already exists
	if val, ok := feed.FareAttributes[fareid]; ok {
		fareattr = val
	} else {
		panic(fmt.Errorf("No fare attribute with id %s found", fareid))
	}

	// create fare attribute
	rule := new(gtfs.FareAttributeRule)

	routeID := prefix + getString(flds.routeId, r, flds, false, false, "")

	if len(routeID) > len(prefix) {
		if val, ok := feed.Routes[routeID]; ok {
			rule.Route = val
		} else {
			panic(fmt.Errorf("No route with id %s found", routeID))
		}
	}

	rule.Origin_id = prefix + getString(flds.originId, r, flds, false, false, "")
	rule.Destination_id = prefix + getString(flds.destinationId, r, flds, false, false, "")
	rule.Contains_id = prefix + getString(flds.containsId, r, flds, false, false, "")

	fareattr.Rules = append(fareattr.Rules, rule)

	return fareattr, rule, nil
}

func createTransfer(r []string, flds TransferFields, feed *Feed, prefix string) (t *gtfs.Transfer, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Transfer)

	if val, ok := feed.Stops[prefix+getString(flds.FromStopId, r, flds, true, true, "")]; ok {
		a.From_stop = val
	} else {
		panic(&StopNotFoundErr{prefix, getString(flds.FromStopId, r, flds, true, true, "")})
	}

	if val, ok := feed.Stops[prefix+getString(flds.ToStopId, r, flds, true, true, "")]; ok {
		a.To_stop = val
	} else {
		panic(&StopNotFoundErr{prefix, getString(flds.ToStopId, r, flds, true, true, "")})
	}

	a.Transfer_type = getRangeInt(flds.TransferType, r, flds, false, 0, 3)
	a.Min_transfer_time = getPositiveIntWithDefault(flds.MinTransferTime, r, flds, -1, feed.opts.UseDefValueOnError, feed)

	return a, nil
}

func createPathway(r []string, flds PathwayFields, feed *Feed, prefix string) (t *gtfs.Pathway, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Pathway)

	a.Id = prefix + getString(flds.pathwayId, r, flds, true, true, "")

	if val, ok := feed.Stops[prefix+getString(flds.fromStopId, r, flds, true, true, "")]; ok {
		a.From_stop = val
		if a.From_stop.Location_type == 1 {
			panic(errors.New("Stop for 'from_stop_id' with id " + getString(flds.fromStopId, r, flds, true, true, "") + " has location_type=1 (Station). Only stops/platforms (location_type=0), entrances/exits (location_type=2), generic nodes (location_type=3) or boarding areas (location_type=4) are allowed here."))
		}
	} else {
		panic(&StopNotFoundErr{prefix, getString(flds.fromStopId, r, flds, true, true, "")})
	}

	if val, ok := feed.Stops[prefix+getString(flds.toStopId, r, flds, true, true, "")]; ok {
		a.To_stop = val
		if a.To_stop.Location_type == 1 {
			panic(errors.New("Stop for 'to_stop_id' with id " + getString(flds.toStopId, r, flds, true, true, "") + " has location_type=1 (Station). Only stops/platforms (location_type=0), entrances/exits (location_type=2), generic nodes (location_type=3) or boarding areas (location_type=4) are allowed here."))
		}
	} else {
		panic(&StopNotFoundErr{prefix, getString(flds.toStopId, r, flds, true, true, "")})
	}

	a.Mode = uint8(getRangeInt(flds.pathwayMode, r, flds, true, 1, 7))
	a.Is_bidirectional = getBool(flds.isBidirectional, r, flds, true, false, feed.opts.UseDefValueOnError, feed)

	length := getNullableFloat(flds.length, r, flds, feed.opts.UseDefValueOnError, feed)
	a.Length = length

	a.Traversal_time = int(getPositiveIntWithDefault(flds.traversalTime, r, flds, -1, feed.opts.UseDefValueOnError, feed))

	a.Stair_count = getIntWithDefault(flds.stairCount, r, flds, 0, feed.opts.UseDefValueOnError, feed)
	a.Max_slope = getNullableFloat(flds.maxSlope, r, flds, feed.opts.UseDefValueOnError, feed)
	if math.IsNaN(float64(a.Max_slope)) {
		a.Max_slope = 0
	}

	width := getNullablePositiveFloat(flds.minWidth, r, flds, feed.opts.UseDefValueOnError, feed)
	a.Min_width = width

	a.Signposted_as = getString(flds.signpostedAs, r, flds, false, false, "")
	a.Reversed_signposted_as = getString(flds.reversedSignpostedAs, r, flds, false, false, "")

	return a, nil
}

func createLevel(r []string, flds LevelFields, feed *Feed, idprefix string) (t *gtfs.Level, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	a := new(gtfs.Level)

	a.Id = idprefix + getString(flds.levelId, r, flds, true, true, "")
	a.Index = getNullableFloat(flds.levelIndex, r, flds, feed.opts.UseDefValueOnError, feed)
	if math.IsNaN(float64(a.Index)) {
		a.Index = 0
	}
	a.Name = getString(flds.levelName, r, flds, false, false, "")

	return a, nil
}

func getString(id int, r []string, flds Fields, req bool, nonempty bool, emptyrepl string) string {
	if id >= 0 {
		val := ""
		if id < len(r) {
			val = r[id]
		}
		trimmed := strings.TrimSpace(val)
		if nonempty && len(trimmed) == 0 {
			if len(emptyrepl) > 0 {
				return emptyrepl
			} else {
				panic(fmt.Errorf("Expected non-empty string for field '%s'", flds.FldName(id)))
			}
		} else {
			return trimmed
		}
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	return ""
}

func trimQuotes(s string) string {
	return strings.TrimSpace(strings.Trim(strings.TrimSpace(s), "«»'\"`‹›„“‟”’‘‛"))
}

func getURL(id int, r []string, flds Fields, req bool, ignErrs bool, feed *Feed) *url.URL {
	val := ""
	if id >= 0 && id < len(r) {
		val = r[id]
	}
	if len(trimQuotes(val)) > 0 {
		u, e := url.ParseRequestURI(trimQuotes(val))

		// try out various heuristics
		if e != nil {
			u, e = url.ParseRequestURI("http://" + trimQuotes(val))
		}

		if e != nil {
			// full URL somewhere inside the field
			pattern := regexp.MustCompile(`https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)`)

			u, e = url.ParseRequestURI(pattern.FindString(val))
		}

		if e != nil {
			// url without http/s somewhere inside the field
			pattern := regexp.MustCompile(`[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)`)
			found := pattern.FindString(val)

			if len(found) > 0 {
				found = "http://" + found
				u, e = url.ParseRequestURI(found)
			}
		}

		if e != nil {
			locErr := fmt.Errorf("'%s' is not a valid url", errFldPrep(strings.TrimSpace(val)))
			if req || !ignErrs {
				panic(locErr)
			} else {
				feed.warn(locErr)
				return nil
			}
		}
		return u
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	return nil
}

func getMail(id int, r []string, flds Fields, req bool, ignErrs bool, feed *Feed) *mail.Address {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
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
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	return nil
}

func getTimezone(id int, r []string, flds Fields, req bool, ignErrs bool, feed *Feed) gtfs.Timezone {

	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		tz, e := gtfs.NewTimezone(val)
		if e != nil && (req || !ignErrs) {
			panic(e)
		} else if e != nil {
			feed.warn(e)
			return tz
		}
		return tz
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	tz, _ := gtfs.NewTimezone("")
	return tz
}

func getIsoLangCode(id int, r []string, flds Fields, req bool, ignErrs bool, feed *Feed) gtfs.LanguageISO6391 {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		l, e := gtfs.NewLanguageISO6391(strings.TrimSpace(val))
		if e != nil && (req || !ignErrs) {
			panic(e)
		} else if e != nil {
			feed.warn(e)
			return l
		}
		return l
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	l, _ := gtfs.NewLanguageISO6391("")
	return l
}

func getColor(id int, r []string, flds Fields, req bool, def string, ignErrs bool, feed *Feed) string {
	val := ""
	if id >= 0 && id < len(r) {
		val = r[id]
	}
	if len(val) > 0 {
		val = strings.TrimSpace(val)
		if len(val) != 6 {
			locErr := fmt.Errorf("Expected six-character hexadecimal number as color for field '%s' (found: %s)", flds.FldName(id), errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}

		if _, e := hex.DecodeString(val); e != nil {
			locErr := fmt.Errorf("Expected hexadecimal number as color for field '%s' (found: %s)", flds.FldName(id), val)
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}
		return strings.ToUpper(val)
	} else if req {
		locErr := fmt.Errorf("Expected required field '%s'", flds.FldName(id))
		if ignErrs {
			feed.warn(locErr)
			return def
		}
		panic(locErr)
	}
	return strings.ToUpper(def)
}

func getIntWithDefault(id int, r []string, flds Fields, def int, ignErrs bool, feed *Feed) int {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			locErr := fmt.Errorf("Expected integer for field '%s', found '%s'", flds.FldName(id), errFldPrep(val))
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

func getPositiveInt(id int, r []string, flds Fields, req bool) int {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || num < 0 {
			panic(fmt.Errorf("Expected positive integer for field '%s', found '%s'", flds.FldName(id), errFldPrep(val)))
		}
		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	return 0
}

func getPositiveIntWithDefault(id int, r []string, flds Fields, def int, ignErrs bool, feed *Feed) int {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || num < 0 {
			locErr := fmt.Errorf("Expected positive integer for field '%s', found '%s'", flds.FldName(id), errFldPrep(val))
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

func getRangeInt(id int, r []string, flds Fields, req bool, min int, max int) int {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			panic(fmt.Errorf("Expected integer for field '%s', found '%s'", flds.FldName(id), errFldPrep(val)))
		}

		if num > max || num < min {
			panic(fmt.Errorf("Expected integer between %d and %d for field '%s', found %s", min, max, flds.FldName(id), errFldPrep(val)))
		}

		return num
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	return 0
}

func getRangeIntWithDefault(id int, r []string, flds Fields, min int, max int, def int, ignErrs bool, feed *Feed) int {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			locErr := fmt.Errorf("Expected integer for field '%s', found '%s'", flds.FldName(id), errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}

		if num > max || num < min {
			locErr := fmt.Errorf("Expected integer between %d and %d for field '%s', found %s", min, max, flds.FldName(id), errFldPrep(val))
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

func getFloat(id int, r []string, flds Fields, req bool) float32 {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		trimmed := strings.TrimSpace(val)
		num, err := strconv.ParseFloat(trimmed, 32)
		if err != nil {
			// try with comma as decimal separator
			num, err = strconv.ParseFloat(strings.Replace(trimmed, ",", ".", 1), 32)
		}
		if err != nil {
			panic(fmt.Errorf("Expected float for field '%s', found '%s'", flds.FldName(id), errFldPrep(val)))
		}
		return float32(num)
	} else if req {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}
	return -1
}

func getTime(id int, r []string, flds Fields) gtfs.Time {
	if id < 0 {
		panic(fmt.Errorf("Expected required field '%s'", flds.FldName(id)))
	}

	str := ""
	if id < len(r) {
		str = strings.TrimSpace(r[id])
	}

	if len(str) == 0 {
		return gtfs.Time{Second: int8(-1), Minute: int8(-1), Hour: int8(-1)}
	}

	var hour, minute, second int
	parts := strings.Split(str, ":")
	var e error

	if len(parts) != 3 || len(parts[0]) == 0 || len(parts[1]) != 2 || len(parts[2]) != 2 {
		e = fmt.Errorf("Expected HH:MM:SS time for field '%s', found '%s' (%s)", flds.FldName(id), errFldPrep(str), e.Error())
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
		panic(fmt.Errorf("Expected HH:MM:SS time for field '%s', found '%s' (%s)", flds.FldName(id), errFldPrep(str), e.Error()))
	} else {
		return gtfs.Time{Hour: int8(hour), Minute: int8(minute), Second: int8(second)}
	}
}

func getNullablePositiveFloat(id int, r []string, flds Fields, ignErrs bool, feed *Feed) float32 {
	val := ""
	if id >= 0 && id < len(r) {
		val = r[id]
	}
	if len(val) > 0 {
		num, err := strconv.ParseFloat(strings.TrimSpace(val), 32)
		if err != nil || num < 0 {
			locErr := fmt.Errorf("Expected positive float for field '%s', found '%s'", flds.FldName(id), errFldPrep(val))
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

func getNullableFloat(id int, r []string, flds Fields, ignErrs bool, feed *Feed) float32 {
	val := ""
	if id >= 0 && id < len(r) {
		val = r[id]
	}
	if len(val) > 0 {
		num, err := strconv.ParseFloat(strings.TrimSpace(val), 32)
		if err != nil {
			locErr := fmt.Errorf("Expected float for field '%s', found '%s'", flds.FldName(id), errFldPrep(val))
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

func getBool(id int, r []string, flds Fields, req bool, def bool, ignErrs bool, feed *Feed) bool {
	val := ""
	if id >= 0 && id < len(r) {
		val = strings.TrimSpace(r[id])
	}
	if len(val) > 0 {
		num, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || (num != 0 && num != 1) {
			locErr := fmt.Errorf("Expected 1 or 0 for field '%s', found '%s'", flds.FldName(id), errFldPrep(val))
			if ignErrs {
				feed.warn(locErr)
				return def
			}
			panic(locErr)
		}
		return num == 1
	} else if req {
		locErr := fmt.Errorf("Expected required field '%s'", flds.FldName(id))
		if ignErrs {
			feed.warn(locErr)
			return def
		}
		panic(locErr)
	}
	return def
}

func getDate(id int, r []string, flds Fields, req bool, ignErrs bool, feed *Feed) gtfs.Date {
	if id < 0 || id >= len(r) || len(r[id]) == 0 {
		if req {
			locErr := fmt.Errorf("Expected required field '%s'", flds.FldName(id))
			if ignErrs {
				feed.warn(locErr)
				return gtfs.Date{Day: 0, Month: 0, Year: 0}
			}
			panic(locErr)
		} else {
			return gtfs.Date{Day: 0, Month: 0, Year: 0}
		}
	}

	str := r[id]

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
		locErr := fmt.Errorf("Expected YYYYMMDD date for field '%s', found '%s' (%s)", flds.FldName(id), errFldPrep(str), e.Error())
		if !ignErrs {
			panic(locErr)
		}
		feed.warn(locErr)
	}
	return gtfs.Date{Day: int8(day), Month: int8(month), Year: int16(year)}
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
