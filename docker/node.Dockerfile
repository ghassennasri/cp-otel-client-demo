# Image of a "CP host": Ubuntu 24.04 + systemd + Java 21, used like a VM.
# Confluent Platform itself is NOT in the image; cp-ansible installs it at deploy time.
# The image only adds:
#   - the otelcol-contrib binary (the agent is configured by ansible/deploy-otel.yml),
#   - the KIP-714 broker plugin JAR (optional part of the demo).

# Build the KIP-714 broker plugin.
FROM maven:3.9.11-eclipse-temurin-21 AS reporter
WORKDIR /build
COPY plugins/client-telemetry-reporter/pom.xml ./pom.xml
RUN mvn -B -ntp dependency:go-offline
COPY plugins/client-telemetry-reporter/src ./src
RUN mvn -B -ntp package -DskipTests

# Take the pinned collector binary from the official image.
FROM otel/opentelemetry-collector-contrib:0.162.0-amd64 AS otel
FROM ubuntu:24.04
ENV container=docker DEBIAN_FRONTEND=noninteractive LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends systemd systemd-sysv dbus python3 python3-apt sudo curl ca-certificates gnupg gpg apt-transport-https procps iproute2 net-tools unzip tar locales acl rsync openjdk-21-jdk-headless && locale-gen en_US.UTF-8 && rm -rf /var/lib/apt/lists/*
COPY --from=otel /otelcol-contrib /usr/local/bin/otelcol-contrib
COPY --from=reporter /build/target/client-telemetry-reporter.jar /usr/share/java/kafka/client-telemetry-reporter.jar
STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]
