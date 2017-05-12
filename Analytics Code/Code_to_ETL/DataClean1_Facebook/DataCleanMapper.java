/**
 * Created by yunjianyang on 11/16/16.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataCleanMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words = line.split(" ");
        if(words[0].length() < 16 && words[0].length() >= 1) //Make sure the length of first key is less than 16
        context.write(new Text(words[0]), new Text(words[1])); // Make sure the first key is not null
    }
}
