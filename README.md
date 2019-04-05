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
>Spark task serialization issues are a challenge, to put it mildly. Earlier versions of this app relied
too much on a task closure accessing external hbase and h2 proxies. The current implementation temporarily
creates a pre-Spark session hbase and h2 proxy. Only after all pre-Spark session work has been completed,
will a Spark session be created. Then a Dataset is created from a suquence of pre-scanned HBase row keys.
Then an hbase and h2 proxy are created within the Spark task closure, with intention that all hbase and h2
code will execute on a Spark worker node.

Pre Spark Session
-----------------
1. Create HBase and H2 proxies.
2. Create HBase key-value table.
3. Put key-value pairs into HBase key-value table.
4. Scan HBase key-value table for all row keys.
5. Create H2 key-value table.

Spark Session
-------------
1. Create HBase and H2 proxies.
2. Create Dataset from sequence of HBase row keys.
3. Foreach row key Get Json value via HBase client.
4. Convert Json value to Scala object.
5. Insert Scala object into key-value H2 table.
6. Update Scala object in H2 key-value table.

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