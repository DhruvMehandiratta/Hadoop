import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Q1Reducer extends Reducer<TextPair, Text, Text, Text> {
	static String movies_file_path = Constants.MOVIES_FILE_PATH;
	private MovieMetadata metadata;
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try {
			metadata = new MovieMetadata();
			metadata.initialize(new File(movies_file_path));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		Text participantName = new Text(iter.next());
		while (iter.hasNext()) {
			Text rolesRecordForThisParticipant = iter.next();
			String[] rolesRecords = rolesRecordForThisParticipant.toString().split("\t");
			String thisCategory=rolesRecords[3];
			try{
				if(thisCategory.equals(Constants.DIRECTOR) && metadata.getMovieReleaseYear(rolesRecords[0]).matches("-?\\d+") && Integer.parseInt(metadata.getMovieReleaseYear(rolesRecords[0])) >2010 && metadata.getMovieGenres(rolesRecords[0]).contains(Constants.WESTERN))
				{
					Text outValue1 = new Text(participantName.toString()+"\t\t"+metadata.getMovieTitle(rolesRecords[0])+"\t\t");
					Text outValue2 = new Text(metadata.getMovieReleaseYear(rolesRecords[0].toString()));
					context.write(outValue1, outValue2);}}
			catch(NullPointerException e){

			}
		}
	} 
}