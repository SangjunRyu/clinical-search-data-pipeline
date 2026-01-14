FROM bitnamilegacy/spark:3.4.1

USER root

# Kafka / JDBC 커넥터 JAR 추가
COPY jars /opt/spark/jars

# 작업 디렉토리 권한 보장
RUN chown -R 1001:1001 /opt/spark

USER 1001

