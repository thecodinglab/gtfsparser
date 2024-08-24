// Copyright 2020 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	mail "net/mail"
	url "net/url"
)

// An Attribution represents attribution parameters
type Attribution struct {
	ID               string
	OrganizationName string
	IsProducer       bool
	IsOperator       bool
	IsAuthority      bool
	Email            *mail.Address
	URL              *url.URL
	Phone            string
}
