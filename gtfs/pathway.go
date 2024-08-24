// Copyright 2019 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Pathway represents a graph of the station layout
type Pathway struct {
	ID                   string
	FromStop             *Stop
	ToStop               *Stop
	Mode                 uint8
	IsBidirectional      bool
	Length               float32
	TraversalTime        int
	StairCount           int
	MaxSlope             float32
	MinWidth             float32
	SignpostedAs         string
	ReversedSignpostedAs string
	Translations         []*Translation
}
