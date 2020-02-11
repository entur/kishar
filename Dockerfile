FROM openjdk:11-jre
ADD target/kishar-*-SNAPSHOT.jar kishar.jar

EXPOSE 8888
CMD java $JAVA_OPTIONS -jar /kishar.jar