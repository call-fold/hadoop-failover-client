## Hadoop Failover Client

----

#### A new failover to cache active namenode index to avoid trying to connect to each namenode by hashcode

#### requirement
setting in hdfs-site.xml
```xml
 <!-- namespace can be found in core-site.xml (fs.defaultFS) -->
<property>
  <name>dfs.client.failover.proxy.provider.[namespace]</name>
  <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProviderCached</value>
</property>
```
when you submit a mapreduce or spark application to yarn, if you set this in client, you should also upload the compiled jar to hdfs and add it to task classpath

----