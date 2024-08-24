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

// FeedInfo holds general information about a GTFS feed
type FeedInfo struct {
	PublisherName string
	PublisherURL  *url.URL
	Lang          string
	StartDate     Date
	EndDate       Date
	Version       string
	ContactEmail  *mail.Address
	ContactURL    *url.URL
}
