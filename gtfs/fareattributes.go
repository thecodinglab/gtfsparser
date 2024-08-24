// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A FareAttribute is a single fare attribute that applies if
// certain FareAttributeRules are matched
type FareAttribute struct {
	ID               string
	Price            string
	CurrencyType     string
	PaymentMethod    int
	Transfers        int
	Agency           *Agency
	TransferDuration int
	Rules            []*FareAttributeRule
}

// A FareAttributeRule holds rules which describe when a
// FareAttribute applies
type FareAttributeRule struct {
	Route         *Route
	OriginID      string // connection to ZoneID in Stop
	DestinationID string // connection to ZoneID in Stop
	ContainsID    string // connection to ZoneID in Stop
}
