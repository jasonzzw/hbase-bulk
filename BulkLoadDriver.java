import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import java.lang.*;

/**
 * Created by shaobo on 15-6-9.
 */
public class BulkLoadDriver extends Configured implements Tool {
    private String tableName = "test";
    
    public static void main(String[] args) {
        try {
            int response = ToolRunner.run(HBaseConfiguration.create(), new BulkLoadDriver(), args);
            if(response == 0) {
                System.out.println("Job is successfully completed...");
            } else {
                System.out.println("Job failed...");
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        String outputPath = args[2];
		long splitSize=Long.parseLong(args[3]);
		tableName=args[0];
        /**
         * 设置作业参数
         */
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "Bulk Loading HBase Table::" + tableName);
        job.setJarByClass(BulkLoadDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
		
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);//指定输出键类
        job.setMapOutputValueClass(Put.class);//指定输出值类
        job.setMapperClass(BulkLoadMapper.class);//指定Map函数
       
        FileInputFormat.addInputPaths(job, args[1]);//输入路径
		FileInputFormat.setMinInputSplitSize(job, splitSize);
        FileSystem fs = FileSystem.get(configuration);
        Path output = new Path(outputPath);
        if (fs.exists(output)) {
            fs.delete(output, true);//如果输出路径存在，就将其删除
        }
        FileOutputFormat.setOutputPath(job, output);//输出路径
        Connection connection = ConnectionFactory.createConnection(configuration);
        TableName tn = TableName.valueOf(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tn), connection.getRegionLocator(tn));
        job.waitForCompletion(true);
        if (job.isSuccessful()){
            HFileLoader.doBulkLoad(outputPath, tableName);//导入数据
            return 0;
        } else {
            return 1;
        }
    }

}
