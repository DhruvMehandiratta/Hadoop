import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Q2Reducer extends Reducer<TextPair, Text, Text, Text> {
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
		Iterator<Text> myIter = values.iterator();
		String participantName = myIter.next().toString();
		HashMap<String, String> peopleMapper = new HashMap<String, String>();
		//stores key:nconst+tconst, value:category
		while(myIter.hasNext()){
			Text rolesRecordForThisParticipant = myIter.next();
			String[] rolesRecords = rolesRecordForThisParticipant.toString().split("\t");
			String tconst = rolesRecords[0];
			String thisCategory = rolesRecords[1];
			if(!peopleMapper.containsKey(key.getFirst().toString()+"\t"+tconst)){
				//if new participant, store it.    key = nconst_tcosnt value=category
				peopleMapper.put(key.getFirst().toString()+"\t"+tconst, thisCategory);
			}else{
				//already stored this key (person + movie)
				if(!thisCategory.equals(peopleMapper.get(key.getFirst().toString()+"\t"+tconst))){
					//same key but different role of the person = ideal candidate
					try{
						//check if one of the roles is director or not
						if(thisCategory.equals(Constants.DIRECTOR) || peopleMapper.get(key.getFirst().toString()+"\t"+tconst).equals(Constants.DIRECTOR)){
							context.write(new Text(participantName), new Text(metadata.getMovieTitle(tconst)));
							//this condition will take care of actor and actress pairs
						}
					}
					catch(Exception e){

					}
				}
			}

		}
	} 
}