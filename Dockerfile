FROM eclipse-temurin:21.0.7_6-jdk-alpine

RUN addgroup appuser && adduser --disabled-password appuser --ingroup appuser
USER appuser

WORKDIR /home/appuser

RUN chown -R appuser:appuser /home/appuser
USER appuser

ADD target/kishar-*-SNAPSHOT.jar kishar.jar

EXPOSE 8888
CMD java $JAVA_OPTIONS -jar /kishar.jar