# ---- Stage 1: build the fat jar with Maven (no local Maven needed) ----
# Build context is the repo root so we can pull in the shared proto/ dir.
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build

# Dependencies first for layer caching.
COPY flink-job/pom.xml .
RUN mvn -q -e -B dependency:go-offline || true

# Shared schema + sources, then package.
COPY proto/ ./proto/
COPY flink-job/src/ ./src/
RUN mvn -q -e -B clean package

# ---- Stage 2: runtime image (Flink + our job, application mode) ----
FROM apache/flink:1.18.1-java17

# Enable the S3 filesystem plugin so the Parquet sink can write to MinIO via
# s3a://. Flink loads filesystems from plugins/, not the job classpath.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/

# Application-mode: the job jar lives in usrlib and the JobManager runs it
# directly (see docker-compose "standalone-job" command).
COPY --from=builder /build/target/log-processor.jar /opt/flink/usrlib/log-processor.jar
