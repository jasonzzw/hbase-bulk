#parameters:
#$1: table name
#$2: input path
#$3: output path
#$4: split size, to control the number of mappers
HADOOP_CLASSPATH=`hbase classpath`:BulkLoad.jar hadoop jar BulkLoad.jar  -Dmapreduce.reduce.memory.mb=20000  $1 $2 $3 $4 -libjars gson-2.2.2.jar
