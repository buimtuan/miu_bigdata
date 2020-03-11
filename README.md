hadoop jar ./target/bd-1.0-SNAPSHOT-jar-with-dependencies.jar bd.WordCount /bd/input /bd/wordcount_output
hadoop jar ./target/bd-1.0-SNAPSHOT-jar-with-dependencies.jar bd.ApacheLog /bd/apache /bd/output
hadoop fs -cat /bd/output/*
