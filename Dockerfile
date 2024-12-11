FROM eclipse-temurin:11-jdk AS builder

WORKDIR /app

COPY build.gradle gradlew /app/
COPY gradle /app/gradle

COPY . /app/

RUN /app/gradlew clean assemble

FROM flink:1.19.1-java11

COPY --from=builder /app/build/libs/*.jar /opt/flink/jobs/flink-bootstrap-function-issue.jar

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.19.1.jar /opt/flink/plugins/flink-s3-fs-hadoop/flink-s3-fs-hadoop-1.19.1.jar && \
    rm -rf /opt/flink/opt
