import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.*;
import java.util.*;
import com.google.gson.*;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private JsonParser parser = new JsonParser();

    public void map(LongWritable key, Text value, Context context){
        String rowKey="";
        try {
            String[] values = value.toString().split("\t");
            if (values.length!=2){
                return;
            }
            
            String jstr=values[1];
            JsonObject element = (JsonObject)parser.parse(jstr);
 
            rowKey=element.get("sid").toString()+":"+element.get("iid").toString();
            System.err.println(rowKey);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("sid"), Bytes.toBytes(element.get("sid").toString()));
            put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("iid"), Bytes.toBytes(element.get("iid").toString()));
            put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("gtime"), Bytes.toBytes(element.get("gtime").toString()));
            if(element.get("list")!=null){
                put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("list"), Bytes.toBytes(element.get("list").toString()));
            }
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
        } catch(Exception exception) {
            System.err.println("#error data: "+rowKey);
            exception.printStackTrace();
        }
    }
}
