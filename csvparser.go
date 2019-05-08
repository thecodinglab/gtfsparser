// Copyright 2015 geOps
// Authors: patrick.brosi@geops.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfsparser

import (
	"encoding/csv"
	"io"
)

// CsvParser is a wrapper around csv.Reader
type CsvParser struct {
	header     []string
	reader     *csv.Reader
	Curline    int
	silentfail bool
}

// NewCsvParser creates a new CsvParser
func NewCsvParser(file io.Reader, silentfail bool) CsvParser {
	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1
	p := CsvParser{reader: reader}
	p.parseHeader()
	p.silentfail = silentfail

	return p
}

// ParseRecord reads a single line into a map
func (p *CsvParser) ParseRecord() map[string]string {
	l := p.parseCsvLine()

	if l == nil {
		return nil
	}

	record := make(map[string]string)

	for i, e := range p.header {
		if i >= len(l) {
			record[e] = ""
		} else {
			record[e] = l[i]
		}
	}

	return record
}

func (p *CsvParser) parseCsvLine() []string {
	record, err := p.reader.Read()
	p.Curline++

	// handle byte order marks
	if len(record) > 0 {
		// utf 8
		if len(record[0]) > 2 && record[0][0] == 239 && record[0][1] == 187 && record[0][2] == 191 {
			record[0] = record[0][3:]

			// utf 16 BE
		} else if len(record[0]) > 1 && record[0][0] == 254 && record[0][1] == 255 {
			record[0] = record[0][2:]

			// utf 16 LE
		} else if len(record[0]) > 1 && record[0][0] == 255 && record[0][1] == 254 {
			record[0] = record[0][2:]
		}
	}

	if err == io.EOF {
		return nil
	} else if err != nil {
		if p.silentfail {
			return nil
		} else {
			panic(err)
		}
	}
	return record
}

func (p *CsvParser) parseHeader() {
	p.header = p.parseCsvLine()
}
