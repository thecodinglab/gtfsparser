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
	Id                string
	Organization_name string
	Is_producer       bool
	Is_operator       bool
	Is_authority      bool
	Email             *mail.Address
	Url               *url.URL
	Phone             string
}
