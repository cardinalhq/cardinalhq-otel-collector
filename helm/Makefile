# Copyright 2024 CardinalHQ, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

release:
	helm package cardinalhq-otel-collector

push:
	helm push cardinalhq-otel-collector-*.tgz oci://public.ecr.aws/b1d7g2f3 

clean:
	rm -f cardinalhq-otel-collector-*.tgz

template:
	helm template cardinalhq-otel-collector

template-values:
	helm template cardinalhq-otel-collector --values test-values.yaml
