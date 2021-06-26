// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfsparser

import (
	// "github.com/patrickbr/gtfsparser/gtfs"
	"testing"
)

func TestFeedParsing(t *testing.T) {
	feedCorA := NewFeed()
	feedCorA.SetParseOpts(ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false})

	e := feedCorA.Parse("./testfeeds/correct/a")

	if e != nil {
		t.Error(e)
		return
	}

	feedFailA := NewFeed()
	feedFailA.SetParseOpts(ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false})
	e = feedFailA.Parse("./testfeeds/fail/a")

	if e == nil {
		t.Error("Parse successful, but input feed was incorrect!")
		return
	}

	feedFailA = NewFeed()
	feedFailA.SetParseOpts(ParseOptions{UseDefValueOnError: true, DropErroneous: false, DryRun: false})
	e = feedFailA.Parse("./testfeeds/fail/a")

	if e == nil {
		t.Error("Parse successful, but input feed was incorrect - and unfixable via def value!")
		return
	}

	feedFailA = NewFeed()
	feedFailA.SetParseOpts(ParseOptions{UseDefValueOnError: false, DropErroneous: true, DryRun: false})
	e = feedFailA.Parse("./testfeeds/fail/a")

	if e != nil {
		t.Error(e)
		return
	}

	shp, _ := feedFailA.Shapes["C_shp"]

	for i, p := range shp.Points {
		if i > 0 && p.HasDistanceTraveled() && shp.Points[i-1].HasDistanceTraveled() && p.Dist_traveled <= shp.Points[i-1].Dist_traveled {
			t.Error(p.Dist_traveled, shp.Points[i-1].Dist_traveled)
			return
		}
	}

	if len(shp.Points) != 7 {
		t.Error(len(shp.Points))
	}

	feedCorB := NewFeed()
	feedCorB.SetParseOpts(ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false})

	e = feedCorB.Parse("./testfeeds/correct/b")

	if e != nil {
		t.Error(e)
		return
	}
}
