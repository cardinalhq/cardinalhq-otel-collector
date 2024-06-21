// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fingerprinter

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/cespare/xxhash/v2"
	"github.com/db47h/ragel/v2"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter/tokenizer"
)

type Fingerprinter interface {
	IsWord(word string) bool
	Fingerprint(input string) (fingerprint int64, level string, err error)
	TokenizeInput(input string) (string, string, error)
	Tokenize(input string) (string, string, error)
}

type fingerprinterImpl struct {
	wordlist map[string]bool
}

var _ Fingerprinter = (*fingerprinterImpl)(nil)

func NewFingerprinter() *fingerprinterImpl {
	fp := fingerprinterImpl{
		wordlist: make(map[string]bool),
	}
	for _, word := range englishWords {
		fp.wordlist[word] = true
	}
	return &fp
}

func findJSONContent(input string) (string, string, string) {
	var message, jsonContent, extra string
	if strings.Contains(input, "{") && strings.Contains(input, "}") {
		start := strings.Index(input, "{")
		end := strings.LastIndex(input, "}")
		if start < end {
			message = input[:start]
			jsonContent = input[start : end+1]
			extra = input[end+1:]
		}
	}
	return message, jsonContent, extra
}

func getStringKey(v map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := v[k].(string); ok {
			return s
		}
	}
	return ""
}

func (fp *fingerprinterImpl) tokenizeJSONContent(prefix string, data map[string]any, suffix string) (string, string, error) {
	message := getStringKey(data, "message", "msg")
	level := getStringKey(data, "level", "loglevel")
	level = strings.ToLower(level)
	if !slices.Contains(tokenizer.LogLevelNames, level) {
		level = ""
	}

	body := prefix + " " + level + "" + message + " " + suffix + " "
	s, nlevel, err := fp.Tokenize(body)
	if err != nil {
		return "", "", err
	}
	if level == "" {
		level = nlevel
	}
	return s, level, nil
}

func (fp *fingerprinterImpl) Fingerprint(input string) (fingerprint int64, level string, err error) {
	s, level, err := fp.TokenizeInput(input)
	if err != nil {
		return 0, "", err
	}
	return int64(xxhash.Sum64String(s)), level, nil
}

func (fp *fingerprinterImpl) TokenizeInput(input string) (string, string, error) {
	message := strings.TrimSpace(input)

	prefix, jsonContent, suffix := findJSONContent(message)
	if jsonContent != "" {
		var data map[string]any
		if err := json.Unmarshal([]byte(jsonContent), &data); err == nil {
			return fp.tokenizeJSONContent(prefix, data, suffix)
		}
	}

	return fp.Tokenize(message)
}

func (fp *fingerprinterImpl) IsWord(word string) bool {
	if _, ok := fp.wordlist[strings.ToLower(word)]; ok {
		return true
	}
	// If the word is entirely uppercase or entirely lowercase, it needs to fully match.
	if strings.ToUpper(word) == word || strings.ToLower(word) == word {
		return false
	}

	words := splitWords(word)
	for _, w := range words {
		if !fp.IsWord(w) {
			return false
		}
	}
	return true
}

func (fp *fingerprinterImpl) Tokenize(input string) (string, string, error) {
	tk := tokenizer.NewFingerprintTokenizer()
	s := ragel.New("test", strings.NewReader(input), tk)
	items := []string{}
	level := ""
	for {
		_, tok, literal := s.Next()
		switch tok {
		case ragel.EOF:
			return strings.Join(items, " "), strings.ToLower(level), nil
		case ragel.Error:
			return "", "", fmt.Errorf("error: %s", literal)
		case tokenizer.TokenLoglevel:
			if level == "" {
				level = literal
				items = append(items, "<Loglevel>")
			} else {
				items = append(items, strings.ToLower(literal))
			}
		case tokenizer.TokenString:
			if fp.IsWord(literal) {
				items = append(items, strings.ToLower(literal))
			}
		default:
			items = append(items, "<"+tk.TokenString(tok)+">")
		}
	}
}

func splitWords(input string) []string {
	var result []string
	var word strings.Builder

	for i, r := range input {
		// Check if the character is uppercase
		if unicode.IsUpper(r) {
			// If it's not the first character and the previous character is not an underscore,
			// it indicates the start of a new word
			if i != 0 && input[i-1] != '_' {
				result = append(result, word.String())
				word.Reset()
			}
			word.WriteRune(unicode.ToLower(r))
		} else if r == '_' {
			// If underscore is encountered, add the current word to the result
			if word.Len() > 0 {
				result = append(result, word.String())
				word.Reset()
			}
		} else {
			// Append lowercase characters to the current word
			word.WriteRune(r)
		}
	}

	// Add the last word
	if word.Len() > 0 {
		result = append(result, word.String())
	}

	return result
}
