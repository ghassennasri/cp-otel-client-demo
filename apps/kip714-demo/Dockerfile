FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY apps/kip714-demo/pom.xml ./pom.xml
RUN mvn -B -ntp dependency:go-offline
COPY apps/kip714-demo/src ./src
RUN mvn -B -ntp package -DskipTests

FROM eclipse-temurin:21-jre-jammy
RUN useradd --system --uid 10002 --home-dir /app --shell /usr/sbin/nologin kip714-demo \
    && mkdir -p /app \
    && chown -R kip714-demo:kip714-demo /app
COPY --from=build /build/target/kip714-demo.jar /app/application.jar
USER kip714-demo
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/application.jar"]