// Copyright 2017 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfs

import (
	"fmt"
	"strings"
)

var validISO6391 = []string{"ab", "aa", "af", "ak", "sq", "am", "ar", "an", "hy", "as", "av", "ae", "ay", "az", "bm", "ba", "eu", "be", "bn", "bh", "bi", "bs", "br", "bg", "my", "ca", "ch", "ce", "ny", "zh", "cv", "kw", "co", "cr", "hr", "cs", "da", "dv", "nl", "dz", "en", "eo", "et", "ee", "fo", "fj", "fi", "fr", "ff", "gl", "ka", "de", "el", "gn", "gu", "ht", "ha", "he", "hz", "hi", "ho", "hu", "ia", "id", "ie", "ga", "ig", "ik", "io", "is", "it", "iu", "ja", "jv", "kl", "kn", "kr", "ks", "kk", "km", "ki", "rw", "ky", "kv", "kg", "ko", "ku", "kj", "la", "lb", "lg", "li", "ln", "lo", "lt", "lu", "lv", "gv", "mk", "mg", "ms", "ml", "mt", "mi", "mr", "mh", "mn", "na", "nv", "nd", "ne", "ng", "nb", "nn", "no", "ii", "nr", "oc", "oj", "cu", "om", "or", "os", "pa", "pi", "fa", "pl", "ps", "pt", "qu", "rm", "rn", "ro", "ru", "sa", "sc", "sd", "se", "sm", "sg", "sr", "gd", "sn", "si", "sk", "sl", "so", "st", "es", "su", "sw", "ss", "sv", "ta", "te", "tg", "th", "ti", "bo", "tk", "tl", "tn", "to", "tr", "ts", "tt", "tw", "ty", "ug", "uk", "ur", "uz", "ve", "vi", "vo", "wa", "cy", "wo", "fy", "xh", "yi", "yo", "za", "zu"}

// A LanguageISO6391 struct describes a language according to the ISO 6391 standard
type LanguageISO6391 struct {
	l int16
}

// GetLangString returns the two-character string representation of an ISO 6391 language
func (a LanguageISO6391) GetLangString() string {
	if a.l < 0 {
		return ""
	}
	return validISO6391[a.l]
}

// NewLanguageISO6391 create a new LanguageISO6391 object
func NewLanguageISO6391(tofind string) (LanguageISO6391, error) {
	for i, l := range validISO6391 {
		if l == strings.ToLower(tofind) {
			return LanguageISO6391{int16(i)}, nil
		}
	}
	return LanguageISO6391{-1}, fmt.Errorf("'%s' is not a valid ISO 639-1 code, see https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes", tofind)
}
