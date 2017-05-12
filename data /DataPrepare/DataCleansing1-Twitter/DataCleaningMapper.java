import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;

public class DataCleaningMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] token = line.split("\\s+");

        if(token[0]!=null && token[1]!=null && token[0].length()<18 && token[1].length()<18 && isNumeric(token[0]) && isNumeric(token[1])){
            context.write(new Text(token[0]+" "+token[1]), new IntWritable());
        }
        //the tokens store the ID of users and followers in twitter
        //if statement ensure that a token is not null, the longth of a token does not exceed 18, and a token just contains numbers
    }

    public static boolean isNumeric(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(str).matches();
    }
}
