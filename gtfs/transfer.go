// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Transfer describes a rule for making connections between routes at
// certain stops
type Transfer struct {
	From_stop         *Stop
	To_stop           *Stop
	Transfer_type     int
	Min_transfer_time int
}
