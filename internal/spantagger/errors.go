// Copyright 2024-2025 CardinalHQ, Inc
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

package spantagger

type Error string

func (e Error) Error() string { return string(e) }

const (
	InconsistentTraceIDsError = Error("inconsistent traceIDs")
	OrphanedSpanError         = Error("orphaned span")
	NoRootError               = Error("no root span")
	MultipleRootsError        = Error("multiple root spans")
)
