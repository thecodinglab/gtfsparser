// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Trip is a single vehicle trip traveling through
// the network with specific times
type Trip struct {
	Route                *Route
	Service              *Service
	Headsign             *string
	Shape                *Shape
	ID                   string
	ShortName            *string
	BlockID              *string
	StopTimes            StopTimes
	Frequencies          *[]*Frequency
	Attributions         *[]*Attribution
	Translations         *[]*Translation
	DirectionID          int8
	WheelchairAccessible int8
	BikesAllowed         int8
}
