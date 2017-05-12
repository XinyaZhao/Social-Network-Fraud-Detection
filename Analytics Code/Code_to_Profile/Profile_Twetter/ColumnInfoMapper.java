import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ColumnInfoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int i=1;
        for(String token:line.split("\\s+")){
            if(i==1) { 
                context.write(new Text("Column 1 -- Context: UserID    Type: String    Max_Length:"), new IntWritable(token.length()));
            }
            if(i==2) {
                context.write(new Text("Column 2 -- Context: FollowerID   Type: String    Max_Length:"), new IntWritable(token.length()));
            }
            i++;
        }

    }
}
