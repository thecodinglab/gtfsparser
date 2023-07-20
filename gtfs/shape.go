// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	"math"
	"strconv"
)

// A Shape describes the geographical path of one or multiple trips
type Shape struct {
	Id     string
	Points ShapePoints
}

// A ShapePoint is a single point in a Shape
type ShapePoint struct {
	Lat           float32
	Lon           float32
	Sequence      uint32
	Dist_traveled float32
}

// Get a string representation of a ShapePoint
func (p *ShapePoint) String() string {
	return strconv.FormatFloat(float64(p.Lat), 'f', 8, 32) + "," + strconv.FormatFloat(float64(p.Lon), 'f', 8, 32)
}

// Get a string representation of this shape
func (shape *Shape) String() string {
	ret := ""
	first := true
	for _, point := range shape.Points {
		if !first {
			ret += "\n"
		}
		first = false
		ret += point.String()
	}

	return ret
}

// ShapePoints are multiple ShapePoints
type ShapePoints []ShapePoint

func (shapePoints ShapePoints) Len() int {
	return len(shapePoints)
}

func (shapePoints ShapePoints) Less(i, j int) bool {
	return shapePoints[i].Sequence < shapePoints[j].Sequence
}

func (shapePoints ShapePoints) Swap(i, j int) {
	shapePoints[i], shapePoints[j] = shapePoints[j], shapePoints[i]
}

// HasDistanceTraveled returns true if this ShapePoint has a measurement
func (p *ShapePoint) HasDistanceTraveled() bool {
	return !math.IsNaN(float64(p.Dist_traveled))
}
