FROM gradle:8.13.0-jdk21 AS builder

WORKDIR /app

COPY . /app

RUN gradle jar

# Final, minimal image
FROM openjdk:21-jdk-slim

WORKDIR /app

COPY --from=builder /app/core/build/libs/hyperscale.jar /jar/hyperscale.jar

CMD ["java", "-jar", "/jar/hyperscale.jar", "-console"]