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

// nolint

package tokenizer

import (
    "fmt"

    "github.com/db47h/ragel/v2"
)

// Token types
const (
    TokenIdentifier ragel.Token = iota
    TokenString
    TokenUrl
    TokenDuration
    TokenDate
    TokenTime
    TokenNumber
    TokenBool
    TokenLoglevel
    TokenIPv4
    TokenHTTPMethod
    TokenUUID
    TokenEmail
    TokenPath
    TokenFQDN
    TokenISO8601
    TokenModuleName
)

var TokenNames = map[ragel.Token]string{
    TokenIdentifier:  "Identifier",
    TokenString:      "String",
    TokenUrl:         "Url",
    TokenDuration:    "Duration",
    TokenDate:        "Date",
    TokenTime:        "Time",
    TokenNumber:      "Number",
    TokenBool:        "Bool",
    TokenLoglevel:    "Loglevel",
    TokenIPv4:        "IPv4",
    TokenHTTPMethod:  "HTTPMethod",
    TokenUUID:        "UUID",
    TokenEmail:       "Email",
    TokenPath:        "Path",
    TokenFQDN:        "FQDN",
    TokenISO8601:     "ISO8601",
    TokenModuleName:  "ModuleName",
}

var LogLevelNames = []string {
    "trace",
    "debug",
    "info",
    "warn",
    "error",
    "fatal",
    "panic",
}


// make golangci-lint happy
var (
    _ = tokenizer_en_main
    _ = tokenizer_error
)

type FingerprintTokenizer struct {}

func NewFingerprintTokenizer() *FingerprintTokenizer {
	return &FingerprintTokenizer{}
}

func (*FingerprintTokenizer) TokenString(t ragel.Token) string {
    if t < 0 || t >= ragel.Token(len(TokenNames)) {
        return "Token(" + fmt.Sprintf("%d", t) + ")"
    }
    return TokenNames[t]
}

// ragel state machine definition.
%%{
    machine tokenizer;

    # utf-8 support
    include UTF8 "utf8.rl";

    newline = '\n' @{ s.Newline(p) };

    main := |*
        alpha_u = uletter | '_';
        alnum_u = alpha_u | digit;

        number = digit+ ('.' digit+)? | '.' digit+ | digit + '.';

        ansicode = digit+ (';' digit+)*  uletter;

        uuid = xdigit{8} [_\-] (xdigit{4} [_\-]){3} xdigit{12};

        dnslabel = alnum_u+ ('-' alnum_u+)*;
        fqdn = dnslabel ('.' dnslabel)+;
        email = alnum_u+ (('.' | '-' | '+' | '_') alnum_u+)* '@' fqdn;

        path = ('/'{1} (alnum_u | '.')+)+ '/'{0,1};

        durationIdentifier =
            [Nn][Ss] | [Nn] 'ano' | [Nn] 'nano' [Ss] 'econd' 
            | 'us' | 'micro' | 'microsecond'
            | 'ms' | 'mil' | 'mils' | 'milli' | 'millis' | 'millisecond' | 'milliseconds'
            | 's' | 'sec' | 'secs' | 'second' | 'seconds'
            | 'min' | 'mins' | 'minute' | 'minutes'
            | 'hour' | 'hours'
            | 'day' | 'days'
            | 'week' | 'weeks'
            | 'mon' | 'month' | 'months'
            | 'year' | 'years';
        duration = digit+ space* durationIdentifier ('s' | '(s)')?;

        ipv4 = digit{1,3} '.' digit{1,3} '.' digit{1,3} '.' digit{1,3};

        protocol = alnum_u+;
        url_creds = (alnum_u+)? ':' (alnum_u+)? '@';
        url_path = ('/' alnum_u+)*;
        url_host = fqdn | ipv4;
        url_port = ':' digit{1,5};
        url = protocol '://' (url_creds)? url_host? url_port? url_path;
        httpmethod = [Gg][Ee][Tt] | [Pp][Oo][Ss][Tt] | [Pp][Uu][Tt] | [Dd][Ee][Ll][Ee][Tt][Ee] | [Hh][Ee][Aa][Dd] | [Pp][Aa][Tt][Cc][Hh];

        logLevels = [Tt][Rr][Aa][Cc][Ee] | [Dd][Ee][Bb][Uu][Gg] | [Ii][Nn][Ff][Oo] | [Ww][Aa][Rr][Nn] | [Ee][Rr][Rr][Oo][Rr] | [Ff][Aa][Tt][Aa][Ll] | [Pp][Aa][Nn][Ii][Cc];

        brackets = '(' | ')' | '[' | ']' | '{' | '}' | '<' | '>';
        punctuation = '.' | ',' | ';' | ':' | '!' | '?' | '"' | '\'' | '*' | '-' | '_' | '@' | '#' | '$' | '%' | '&' | '^' | '|' | '~' | '`' | '+' | '=' | '\\' | '|';
        skipcharacters = space | newline | cntrl | 0x7f | brackets |  punctuation;

        dateyear = digit{4} | digit{2};
        datemonth = digit{2} | 'jan' | 'feb' | 'mar' | 'apr' | 'may' | 'jun' | 'jul' | 'aug' | 'sep' | 'oct' | 'nov' | 'dec';
        dateday = digit{2};
        datesep = '-' | '/';

        date = dateyear datesep datemonth datesep dateday;
        timesep = ':' | '.';
        iso8601 = digit{4} '-' digit{2} '-' digit{2} 'T' digit{2} ':' digit{2} ':' digit{2} (('.' | ',') digit{1,9})* ('Z' | ('+' | '-') digit{2} ':' digit{2})?;
        time = digit{2} timesep digit{2} timesep digit{2} (('.' | ',') digit{1,9})?;
    
        wordEndOfSentence = [a-zA-Z]+ '. ';

        idchars = alnum_u | '_' | '.' | '-' | '@' | ':';
        identifier = idchars+;

        #identifier = alnum_u+ (('_' | '.' | '-' | '@' | ':')+ alnum_u+)+;

        goModuleAndFile = alnum_u+ '@' (alnum_u | '.' | '-' | '_')+ path ':' digit+;

        # pre-filtering
        ansicode;

        iso8601 {
            s.Emit(ts, TokenISO8601, string(data[ts:te]))
        };

        goModuleAndFile {
            s.Emit(ts, TokenModuleName, string(data[ts:te]))
        };

        path {
            s.Emit(ts, TokenPath, string(data[ts:te]))
        };

        ipv4 {
            s.Emit(ts, TokenIPv4, string(data[ts:te]))
        };

        uuid {
            s.Emit(ts, TokenUUID, string(data[ts:te]))
        };

        url {
            s.Emit(ts, TokenUrl, string(data[ts:te]))
        };

        email {
            s.Emit(ts, TokenEmail, string(data[ts:te]))
        };

        fqdn {
            s.Emit(ts, TokenFQDN, string(data[ts:te]))
        };

        time {
            s.Emit(ts, TokenTime, string(data[ts:te]))
        };

        date {
            s.Emit(ts, TokenDate, string(data[ts:te]))
        };

        duration {
            s.Emit(ts, TokenDuration, string(data[ts:te]))
        };

        number {
            s.Emit(ts, TokenNumber, string(data[ts:te]))
        };

        logLevels {
            s.Emit(ts, TokenLoglevel, string(data[ts:te]))
        };

        logLevels ':' {
            s.Emit(ts, TokenLoglevel, string(data[ts:te-1]))
        };

        httpmethod {
            s.Emit(ts, TokenHTTPMethod, string(data[ts:te]))
        };

        wordEndOfSentence {
            s.Emit(ts, TokenString, string(data[ts:te]))
        };

        identifier ':' {
            s.Emit(ts, TokenIdentifier, string(data[ts:te-1]))
        };

        identifier {
            s.Emit(ts, TokenIdentifier, string(data[ts:te]))
        };

        alpha_u alnum_u* {
            s.Emit(ts, TokenString, string(data[ts:te]))
        };

        skipcharacters+;

        '/';
    *|;
}%%

%%write data nofinal;


func (FingerprintTokenizer) Init(s *ragel.State) (int, int) {
    var cs, ts, te, act int
    %%write init;
    s.SaveVars(cs, ts, te, act)
    return %%{ write start; }%%, %%{ write error; }%%
}

func (FingerprintTokenizer) Run(s *ragel.State, p, pe, eof int) (int, int) {
    cs, ts, te, act, data := s.GetVars()
    %%write exec;
    s.SaveVars(cs, ts, te, act)
    return p, pe
}
