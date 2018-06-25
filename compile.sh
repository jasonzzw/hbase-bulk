CLASSPATH=`hbase classpath`:gson-2.2.2.jar javac *.java
jar cvfm BulkLoad.jar manifest.txt *.class
