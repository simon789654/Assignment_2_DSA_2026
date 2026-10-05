# ---- build stage ----
FROM ballerina/ballerina:latest AS build
WORKDIR /home/ballerina/app
COPY --chown=ballerina . .
RUN bal build

# ---- runtime stage (small JRE image, no Ballerina toolchain) ----
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /home/ballerina/app/target/bin/*.jar app.jar
COPY Config.toml /app/Config.toml
ENV BAL_CONFIG_FILES=/app/Config.toml
ENV JAVA_TOOL_OPTIONS="-Xmx256m"
EXPOSE 8082 9797
ENTRYPOINT ["java", "-jar", "app.jar"]
