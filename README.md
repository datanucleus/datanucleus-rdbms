datanucleus-rdbms
=================

DataNucleus support for persistence to RDBMS Datastores. This plugin makes use
of JDBC drivers for the datastores supported. Each supported datastore will have an associated "adapter"
stored under <a href="https://github.com/datanucleus/datanucleus-rdbms/tree/master/src/java/org/datanucleus/store/rdbms/adapter">org.datanucleus.store.rdbms.adapter</a>, 
so if planning on supporting or improving support for a datastore this is the place to look (as well as in 
<a href="https://github.com/datanucleus/datanucleus-rdbms/blob/master/plugin.xml">plugin.xml</a>).

This project is built using Maven, by executing `mvn clean install` which installs the built jar in your local Maven
repository.

Please refer to http://www.datanucleus.org/plugins/store.rdbms.html  for more information.

License
-------
Apache 2 licensed

Issue Tracker
-------------
http://www.datanucleus.org/servlet/jira/browse/NUCRDBMS
