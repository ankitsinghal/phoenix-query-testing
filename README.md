## Steps to reproduce

1. Creates tables using DDL statements
2. Build driver
```
$ mvn clean package
```
3. Load data, using intended JARs on your local system and `HBASE_CONF_DIR`
```
$ java -cp /usr/local/lib/hbase/conf/:target/phoenix-salt-test-0.0.1-SNAPSHOT.jar:/usr/local/lib/phoenix/phoenix-4.7.0.2.6.5.0-SNAPSHOT-client.jar com.github.joshelser.phoenix.PhoenixTest localhost:2181:/hbase 1000000 50000
```
4. Query data with intended JARs on your local system and `HBASE_CONF_DIR`, zookeeper url , no. of threads, no. of execution of query per thread
```
$ java -Dlog4j.configuration=file:log4j.properties -cp /usr/local/lib/hbase/conf/:target/phoenix-salt-test-0.0.1-SNAPSHOT.jar:/usr/local/lib/phoenix-4.7.0.2.6.3.22-1/phoenix-4.7.0.2.6.3.0-SNAPSHOT-client.jar com.github.joshelser.phoenix.PhoenixQuery localhost:2181:/hbase 6 5
```
