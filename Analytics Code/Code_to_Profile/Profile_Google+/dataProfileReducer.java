import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class dataProReducer
        extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        		int maxValue = Integer.MIN_VALUE; 
        		String x = "subjectId";
        		String y = "followerId";
        		for (IntWritable value : values) {
            		maxValue = Math.max(maxValue, value.get());
            	}
        		if (key.equals(new IntWritable(1)))
        			context.write(new Text(x), new IntWritable(maxValue));
        		else
        			context.write(new Text(y), new IntWritable(maxValue));
    }
}

