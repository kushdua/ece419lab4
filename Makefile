JAVA_HOME=/cad2/ece419s/java/jdk1.6.0/
JAVAC=${JAVA_HOME}/bin/javac -source 1.4
JAVADOC=${JAVA_HOME}/bin/javadoc -use -source 1.6 -author -version -link http://java.sun.com/j2se/1.6.0/docs/api/ 
MKDIR=mkdir
RM=rm -rf
CAT=cat

all: 
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ZkConnector.java Worker.java WorkerThreadHandler.java JT.java FileServer.java BrokerPacket.java Client.java

clean:
	rm -f *.class
	
#java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. worker_draft

#123,dictionary/lowercase.rand,c463be62fd5252c7568d7bafd3cc4a55,0,-;345,dictionary/lowercase.rand,c463be62fd5252c7568d7bafd3cc4a55,0,-;567,dictionary/lowercase.rand,c463be62fd5252c7568d7bafd3cc4a55,0,-;123,dictionary/lowercase.rand,dd81e2cc883fc988c91bd399f87dcb07,0,-;123,dictionary/lowercase.rand,dd81e2cc883fc988c91bd399f874567,0,-;
