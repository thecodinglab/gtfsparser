// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	mail "net/mail"
	url "net/url"
)

// An Agency represents a transit agency in GTFS
type Agency struct {
	ID           string
	Name         string
	URL          *url.URL
	Timezone     Timezone
	Lang         LanguageISO6391
	Phone        string
	FareURL      *url.URL
	Email        *mail.Address
	Attributions []*Attribution
	Translations []*Translation
}
