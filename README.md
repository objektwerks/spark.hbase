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
>Spark task serialization issues are a challenge, to put it mildly. My earlier versions of this app relied
too much on a task closure accessing external hbase and h2 proxies. The current implementation creates an
external hbase and h2 proxy, but then closes them. Now, hbase and h2 proxies are created within the task
closure, with idea of all code executing, as intended, on a worker node.

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