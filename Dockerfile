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

FROM alpine:latest AS certs
RUN apk --update add ca-certificates

FROM scratch

ADD GeoLite2-City.mmdb /app/geoip/GeoLite2-City.mmdb
ENV GEOIP_DB_PATH=/app/geoip/GeoLite2-City.mmdb

ARG USER_UID=2000
USER ${USER_UID}:${USER_UID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=755 cardinalhq-otel-collector /app/bin/cardinalhq-otel-collector
ENTRYPOINT ["/app/bin/cardinalhq-otel-collector"]
CMD ["--config", "/app/config/config.yaml"]
EXPOSE 4317 55678 55679
