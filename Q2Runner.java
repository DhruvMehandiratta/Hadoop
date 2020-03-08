import java.net.URI;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class Q2Runner extends Configured implements Tool{
	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			// 3 input files and 1 output file
			return -1;
		}
		Job job = new Job(getConf(), "Join weather records with station names");
		job.setJarByClass(getClass());
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());	   
		Path namesFilePath = new Path(args[1]);
		Path rolesFilePath = new Path(args[2]);
		Path outputFilePath = new Path(args[3]);
		MultipleInputs.addInputPath(job, namesFilePath,
				TextInputFormat.class, NamesMapper.class);
		MultipleInputs.addInputPath(job, rolesFilePath,
				TextInputFormat.class, Q2RolesMapper.class);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setReducerClass(Q2Reducer.class);
		job.setOutputKeyClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Q2Runner(), args);
		System.exit(exitCode);
	}
}