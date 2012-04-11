JAVA_HOME=/cad2/ece419s/java/jdk1.6.0/
JAVAC=${JAVA_HOME}/bin/javac -source 1.4
JAVADOC=${JAVA_HOME}/bin/javadoc -use -source 1.6 -author -version -link http://java.sun.com/j2se/1.6.0/docs/api/ 
MKDIR=mkdir
RM=rm -rf
CAT=cat

all: 
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar worker_draft.java ZkConnector.java
runWorker:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. worker_draft
clean:
	rm -f *.class
