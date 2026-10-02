FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY apps/customer-observability-demo/pom.xml ./pom.xml
RUN mvn -B -ntp dependency:go-offline
COPY apps/customer-observability-demo/src ./src
RUN mvn -B -ntp package -DskipTests

FROM alpine:3.22 AS agents
RUN apk add --no-cache curl \
    && curl -fsSL -o /opentelemetry-javaagent.jar https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.20.1/opentelemetry-javaagent.jar \
    && curl -fsSL -o /jmx_prometheus_javaagent.jar https://github.com/prometheus/jmx_exporter/releases/download/1.1.0/jmx_prometheus_javaagent-1.1.0.jar

FROM eclipse-temurin:21-jre-jammy
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin customer-demo \
    && mkdir -p /app /opt/agents /var/log/customer-demo \
    && chown -R customer-demo:customer-demo /app /var/log/customer-demo
COPY --from=build /build/target/customer-observability-demo.jar /app/application.jar
COPY --from=agents /opentelemetry-javaagent.jar /opt/agents/opentelemetry-javaagent.jar
COPY --from=agents /jmx_prometheus_javaagent.jar /opt/agents/jmx_prometheus_javaagent.jar
COPY apps/customer-observability-demo/jmx-exporter.yaml /etc/customer-demo/jmx-exporter.yaml
USER customer-demo
EXPOSE 8080 9404
ENTRYPOINT ["java", "-jar", "/app/application.jar"]