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

FROM public.ecr.aws/cardinalhq.io/geoip-base:latest AS geoip

FROM gcr.io/distroless/base-debian12:debug
COPY --from=tools /usr/bin/curl /usr/bin/curl
# Copy all curl dependencies from /lib/*/
COPY --from=tools /lib/*/libcurl.so.4* /lib/
COPY --from=tools /lib/*/libz.so.1* /lib/
COPY --from=tools /lib/*/libnghttp2.so.14* /lib/
COPY --from=tools /lib/*/libidn2.so.0* /lib/
COPY --from=tools /lib/*/librtmp.so.1* /lib/
COPY --from=tools /lib/*/libssh2.so.1* /lib/
COPY --from=tools /lib/*/libpsl.so.5* /lib/
COPY --from=tools /lib/*/libssl.so.3* /lib/
COPY --from=tools /lib/*/libcrypto.so.3* /lib/
COPY --from=tools /lib/*/libgssapi_krb5.so.2* /lib/
COPY --from=tools /lib/*/libldap-2.5.so.0* /lib/
COPY --from=tools /lib/*/liblber-2.5.so.0* /lib/
COPY --from=tools /lib/*/libzstd.so.1* /lib/
COPY --from=tools /lib/*/libbrotlidec.so.1* /lib/
COPY --from=tools /lib/*/libunistring.so.2* /lib/
COPY --from=tools /lib/*/libgnutls.so.30* /lib/
COPY --from=tools /lib/*/libhogweed.so.6* /lib/
COPY --from=tools /lib/*/libnettle.so.8* /lib/
COPY --from=tools /lib/*/libgmp.so.10* /lib/
COPY --from=tools /lib/*/libkrb5.so.3* /lib/
COPY --from=tools /lib/*/libk5crypto.so.3* /lib/
COPY --from=tools /lib/*/libcom_err.so.2* /lib/
COPY --from=tools /lib/*/libkrb5support.so.0* /lib/
COPY --from=tools /lib/*/libsasl2.so.2* /lib/
COPY --from=tools /lib/*/libbrotlicommon.so.1* /lib/
COPY --from=tools /lib/*/libp11-kit.so.0* /lib/
COPY --from=tools /lib/*/libtasn1.so.6* /lib/
COPY --from=tools /lib/*/libkeyutils.so.1* /lib/
COPY --from=tools /lib/*/libresolv.so.2* /lib/
COPY --from=tools /lib/*/libffi.so.8* /lib/
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
