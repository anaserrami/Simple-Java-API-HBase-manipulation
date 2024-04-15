FROM openjdk:8-jdk-alpine
COPY target/ /app.jar
CMD ["java", "-jar", "/app.jar"]