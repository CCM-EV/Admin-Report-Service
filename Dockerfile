# Multi-stage build for Spring Boot application
FROM maven:3.9-eclipse-temurin-17-alpine AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# Add curl for healthcheck
RUN apk add --no-cache curl

EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]
