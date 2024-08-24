// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	"math"
	url "net/url"
)

// A Stop object describes a single stop location
type Stop struct {
	ID                 string
	Code               string
	Name               string
	Desc               string
	Lat                float32
	Lon                float32
	LocationType       int8
	WheelchairBoarding int8
	ZoneID             string
	URL                *url.URL
	ParentStation      *Stop
	Translations       []*Translation
	Level              *Level
	PlatformCode       string
	Timezone           Timezone
}

// HasLatLon returns true if this Stop has a latitude and longitude
func (s *Stop) HasLatLon() bool {
	return !math.IsNaN(float64(s.Lat)) && !math.IsNaN(float64(s.Lon))
}
