// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Trip is a single vehicle trip traveling through
// the network with specific times
type Trip struct {
	Id                    string
	Route                 *Route
	Service               *Service
	Headsign              *string
	Short_name            string
	Direction_id          int8
	Block_id              string
	Shape                 *Shape
	Wheelchair_accessible int8
	Bikes_allowed         int8
	StopTimes             StopTimes
	Frequencies           []*Frequency
	Attributions          []*Attribution
	Translations          []*Translation
}
