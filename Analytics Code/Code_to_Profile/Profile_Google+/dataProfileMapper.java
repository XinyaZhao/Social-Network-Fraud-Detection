
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class dataProMapper
        extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                String line = value.toString();
                StringTokenizer st = new StringTokenizer(line);
                String subjectId = st.nextToken();
                String followerId = st.nextToken();
                context.write (new IntWritable(1), new IntWritable(subjectId.length()));
                context.write (new IntWritable(2), new IntWritable(followerId.length()));
    }
}
