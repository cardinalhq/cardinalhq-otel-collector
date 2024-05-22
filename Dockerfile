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

FROM --platform=${BUILDPLATFORM} golang:latest as build
ARG TARGETOS
ARG TARGETARCH
ARG OTEL_VERSION=0.101.0

WORKDIR /build
COPY . .
RUN go install go.opentelemetry.io/collector/cmd/builder@v${OTEL_VERSION}
ENV GOOS=${TARGETOS} GOARCH=${TARGETARCH}
RUN CGO_ENABLED=0 builder --config=cardinalhq-otel-collector.yaml

FROM gcr.io/distroless/base-debian11 as cardinalhq-otel-collector-image
WORKDIR /app
COPY --from=build /build/bin/cardinalhq-otel-collector /app/bin/cardinalhq-otel-collector

# 4317 - default OTLP receiver
# 55678 - opencensus (tracing) receiver
# 55679 - zpages
EXPOSE 4317/tcp 55678/tcp 55679/tcp
USER 2000:2000
CMD ["/app/bin/cardinalhq-otel-collector"]
