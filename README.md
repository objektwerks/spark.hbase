Spark HBase
-----------
>Project that prototypes a very off-the-wall Spark-HBase-Database app. Avoid at all costs!

Install
-------
>Normally I would use Homebrew to install, start and stop HBase. But, in this case, I strongly recommend
following this guide: http://hbase.apache.org/book.html#quickstart

>If useful, consider adding an **export $HBASE_HOME/bin** to your **export $PATH** entry.

Note
----
>I ran into several Spark task serialization errors. SparkHBaseH2App, for instances, has no read or write Spark
connector, instead relying on the HBase Java Client and JDBC API. See def runJob(...). Initially, I passed H2Proxy
and HBaseProxy into runJob, which always produced task serialization errors. Lesson: A Spark Job, or closure, is
a very restrictive context in which to execute code. Do only what you must in a Spark closure - and no more! That said,
what you do outside of a Spark context will execute on the driver machine. So it's always best to build a normal Spark
app.;)

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