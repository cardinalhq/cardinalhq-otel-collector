# Copyright 2024-2025 CardinalHQ, Inc
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

FROM debian:12-slim AS tools
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
# Use ldd to find curl dependencies and copy them to staging directory
RUN mkdir -p /tmp/curl-deps && \
    ldd /usr/bin/curl | grep "=> /" | awk '{print $3}' | xargs -I {} cp {} /tmp/curl-deps/ 2>/dev/null || true

FROM public.ecr.aws/cardinalhq.io/geoip-base:latest AS geoip

FROM gcr.io/distroless/base-debian12:debug
COPY --from=tools /usr/bin/curl /usr/bin/curl
# Copy all curl dependencies discovered dynamically
COPY --from=tools /tmp/curl-deps/ /lib/
COPY --from=tools /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=geoip /app/geoip /app/geoip
COPY --chmod=755 cardinalhq-otel-collector /app/bin/cardinalhq-otel-collector
COPY --chmod=755 docker/run-with-env-config.sh /app/bin/run-with-env-config

ARG USER_UID=2000
USER ${USER_UID}:${USER_UID}

# The base image has the geoip database in /app/geoip
ENV GEOIP_DB_PATH=/app/geoip/GeoLite2-City.mmdb

# Remove the distroless entrypoint so we can use our own
ENTRYPOINT []
CMD ["/app/bin/cardinalhq-otel-collector", "--config", "/app/config/config.yaml"]
EXPOSE 4317 55678 55679
