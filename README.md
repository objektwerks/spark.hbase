Spark HBase
-----------
>Project that prototypes some Spark-HBase integration ideas.

Install
-------
>Normally I would use Homebrew to install, start and stop HBase. But, in this case, I strongly recommend
following this guide: http://hbase.apache.org/book.html#quickstart

>If useful, consider adding an **export $HBASE_HOME/bin** to your **export $PATH** entry.

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
1. http://localhost:16010/master-status

Stop
----
1. Control-C
 
Output
------
1. ./target/app.log