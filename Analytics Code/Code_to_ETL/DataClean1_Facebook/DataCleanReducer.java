/**
 * Created by yunjianyang on 11/16/16.
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataCleanReducer
        extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for(Text value : values){
            String tmp = value.toString();
            long crt = Long.parseLong(tmp);
            if(tmp.length() < 16 && tmp.length() >= 1) // make sure the second key is not null
            context.write(key, new Text(Long.toString(crt))); //make sure the second key is less than 16
        }
    }
}
