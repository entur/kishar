FROM eclipse-temurin:17-jdk

RUN addgroup appuser && adduser --disabled-password appuser --ingroup appuser
USER appuser

WORKDIR /home/appuser

RUN chown -R appuser:appuser /home/appuser
USER appuser

ADD target/kishar-*-SNAPSHOT.jar kishar.jar

EXPOSE 8888
CMD java $JAVA_OPTIONS -jar /kishar.jar