// Copyright 2020 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

// A Translation holds a single translation for an entity in a GTFS table
type Translation struct {
	FieldName   string
	Language    LanguageISO6391
	Translation string
	FieldValue  string
}
