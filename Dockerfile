FROM gradle:8.9-jdk21 AS builder

WORKDIR /app
COPY . /app

# Build the jar
RUN gradle jar

# Final, minimal image
FROM eclipse-temurin:21

WORKDIR /app
COPY --from=builder /app/core/build/libs/hyperscale.jar /jar/hyperscale.jar

CMD ["java", "-jar", "/jar/hyperscale.jar", "-console"]