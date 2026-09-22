FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY notification-worker/target/notification-worker-*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
