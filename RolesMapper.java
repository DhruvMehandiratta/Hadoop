import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RolesMapper extends  Mapper<LongWritable, Text, TextPair, Text> {	  
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String thisRecordLine = value.toString();
		String[] data= thisRecordLine.split("\t");
		String nconst = data[2];
		context.write(new TextPair(nconst, "1"), value);  //passing roles value as a whole
	}
}