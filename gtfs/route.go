// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	url "net/url"
)

// A Route describes a group of trips that are presented to passangers as
// a single service
type Route struct {
	Id           string
	Agency       *Agency
	Short_name   string
	Long_name    string
	Desc         string
	Type         int16
	Url          *url.URL
	Color        string
	Text_color   string
	Sort_order   int
	Attributions []*Attribution
}

func GetTypeFromExtended(t int16) int16 {
	switch t {
	case 2:
	case 100:
	case 101:
	case 102:
	case 103:
	case 104:
	case 105:
	case 106:
	case 107:
	case 108:
	case 109:
	case 110:
	case 111:
	case 112:
	case 113:
	case 114:
	case 115:
	case 117:
	case 300:
	case 400:
	case 403:
	case 404:
	case 405:
	case 1503:
		return 2 // rail
	case 3:
		return 3 // bus
	case 200:
	case 201:
	case 202:
	case 203:
	case 204:
	case 205:
	case 206:
	case 207:
	case 208:
	case 209:
		return 3 // bus
	case 700:
	case 701:
	case 702:
	case 703:
	case 704:
	case 705:
	case 706:
	case 707:
	case 708:
	case 709:
	case 710:
	case 711:
	case 712:
	case 713:
	case 714:
	case 715:
	case 716:
	case 717:
	case 800:
	case 1500:
	case 1501:
	case 1505:
	case 1506:
	case 1507:
		return 3 // bus
	case 1:
	case 401:
	case 402:
	case 500:
	case 600:
		return 1 // subway
	case 0:
	case 900:
	case 901:
	case 902:
	case 903:
	case 904:
	case 905:
	case 906:
		return 0 // tram
	// TODO(patrick): from here on not complete!
	case 4:
	case 1000:
	case 1200:
	case 1502:
		return 4 // ferry
	case 6:
	case 1300:
	case 1301:
	case 1304:
	case 1306:
	case 1307:
		return 6 // gondola
	case 7:
	case 116:
	case 1303:
	case 1302:
	case 1400:
		return 7 // funicular
	case 5:
		return 5 // cable car
	case 11:
		return 11
	case 12:
		return 12
	}

	return 2 // fallback
}
