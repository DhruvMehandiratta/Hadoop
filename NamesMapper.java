import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class NamesMapper extends  Mapper<LongWritable, Text, TextPair, Text>
{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String thisRecordLine = value.toString();
		String[] data= thisRecordLine.split("\t");
		String nconst = data[0];
		String participantName = data[1];
		context.write(new TextPair(nconst, "0"), new Text(participantName));
	}
}