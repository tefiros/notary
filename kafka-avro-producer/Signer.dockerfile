FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY libs ./libs
COPY src ./src
RUN mvn install:install-file \
      -Dfile=./libs/provenance-api-0.0.4.jar \
      -DgroupId=com.telefonica.api \
      -DartifactId=provenance-api \
      -Dversion=0.0.4 \
      -Dpackaging=jar && \
    mvn clean package -DskipTests -Dmaven.test.skip=true && \
    ls -la target/*.jar

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/kafka-avro-producer-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]