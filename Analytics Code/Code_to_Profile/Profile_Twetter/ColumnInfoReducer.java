import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ColumnInfoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxLen = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxLen = Math.max(maxLen, value.get());
        }
        context.write(key, new IntWritable(maxLen));
    }
}
