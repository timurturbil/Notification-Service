FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY scheduler-worker/target/scheduler-worker-*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
