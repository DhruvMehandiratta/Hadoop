import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q2RolesMapper extends  Mapper<LongWritable, Text, TextPair, Text> {	  
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String thisRecordLine = value.toString();
		String[] data= thisRecordLine.split("\t");
		String nconst = data[2];
		if(data[3].equals(Constants.ACTOR) || data[3].equals(Constants.ACTRESS) || data[3].equals(Constants.DIRECTOR))
		{	
			context.write(new TextPair(nconst, "1"), new Text(data[0]+'\t'+data[3]));
		}
	}
}