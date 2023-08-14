// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Transfer describes a rule for making connections between routes at
// certain stops

type TransferKey struct {
	From_stop  *Stop
	To_stop    *Stop
	From_route *Route
	To_route   *Route
	From_trip  *Trip
	To_trip    *Trip
}

type TransferVal struct {
	Transfer_type     int
	Min_transfer_time int
}
