Spark HBase
-----------
>Project that prototypes some Spark-HBase integration ideas.

Install
-------
>Normally I would use Homebrew to install, start and stop HBase. But, in this case, I strongly recommend
following this guide: http://hbase.apache.org/book.html#quickstart

>If useful, consider adding an **export $HBASE_HOME/bin** to your **export $PATH** entry.

Notes
-----
1. Apache HBase Client - Can't be used in a Spark job because it's not serializable, resulting in this
Spark error: Caused by: java.io.NotSerializableException: org.apache.hadoop.hbase.client.ConnectionImplementation
2. Apache Log4j - Spark can generate this error: java.io.NotSerializableException: org.apache.log4j.Logger Spark
uses log4j exclusively. In the offending class, trait or object extend Serializable ( which does not always work).
3. H2Proxy - When even using the JDBC API, Spark will throw a serialization error, blaming log4j.

HBase
-----
1. hbase/bin$ ./hbase shell

Run
---
1. hbase/bin$ ./start-hbase.sh
2. sbt clean compile run
3. hbase/bin$ ./stop-hbase.sh

Web
---
1. HBase: http://localhost:16010/master-status
2. Spark: http://localhost:4040

Stop
----
1. Control-C
 
Output
------
1. ./target/app.log