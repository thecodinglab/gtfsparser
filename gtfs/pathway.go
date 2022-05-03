// Copyright 2019 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Pathway represents a graph of the station layout
type Pathway struct {
	Id                     string
	From_stop              *Stop
	To_stop                *Stop
	Mode                   uint8
	Is_bidirectional       bool
	Length                 float32
	Traversal_time         int
	Stair_count            int
	Max_slope              float32
	Min_width              float32
	Signposted_as          string
	Reversed_signposted_as string
	Translations           []*Translation
}
