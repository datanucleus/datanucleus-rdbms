datanucleus-rdbms
=================

DataNucleus support for persistence to RDBMS Datastores. This plugin makes use
of JDBC drivers for the datastores supported. Each supported datastore will have an associated "adapter"
stored under <a href="https://github.com/datanucleus/datanucleus-rdbms/tree/master/src/java/org/datanucleus/store/rdbms/adapter">org.datanucleus.store.rdbms.adapter</a>, 
so if planning on supporting or improving support for a datastore this is the place to look (as well as in 
<a href="https://github.com/datanucleus/datanucleus-rdbms/blob/master/plugin.xml">plugin.xml</a>).

This project is built using Maven, by executing `mvn clean install` which installs the built jar in your local Maven
repository.


KeyFacts
--------
__License__ : Apache 2 licensed

__Issue Tracker__ : http://www.datanucleus.org/servlet/jira/browse/NUCRDBMS

__RoadMap__ : http://issues.datanucleus.org/browse/NUCRDBMS?report=com.atlassian.jira.plugin.system.project:roadmap-panel

__Javadocs__ : [3.2](http://www.datanucleus.org/javadocs/store.rdbms/3.2/), [3.1](http://www.datanucleus.org/javadocs/store.rdbms/3.1/), [3.0](http://www.datanucleus.org/javadocs/store.rdbms/3.0/), [2.2](http://www.datanucleus.org/javadocs/store.rdbms/2.2/), [2.1](http://www.datanucleus.org/javadocs/store.rdbms/2.1/), [2.0](http://www.datanucleus.org/javadocs/store.rdbms/2.0/), [1.1](http://www.datanucleus.org/javadocs/store.rdbms/1.1/), [1.0](http://www.datanucleus.org/javadocs/store.rdbms/1.0/)

__Download(Releases)__ : [Maven Central](http://central.maven.org/maven2/org/datanucleus/datanucleus-rdbms)

__Download(Nightly)__ : [Nightly Builds](http://www.datanucleus.org/downloads/maven2-nightly/org/datanucleus/datanucleus-rdbms)

__Dependencies__ : See file [pom.xml](pom.xml)
