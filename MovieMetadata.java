import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IOUtils;

public class MovieMetadata {
	private Map<String, String> movieMap = new HashMap<String, String>();
	public void initialize(File file) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			MovieMetadataParser parser = new MovieMetadataParser();
			String line;
			while ((line = in.readLine()) != null) {
				if (parser.parse(line)) {
					movieMap.put(parser.getMovieID(), parser.getMovieTitle()+"\t"+parser.getMovieReleaseYear()+"\t"+parser.getMovieGenres());
				}
			}
		} finally {
			IOUtils.closeStream(in);
		}
	}
	public String getMovieDetails(String tconst) {
		String val = movieMap.get(tconst);
		if (val == null || val.trim().length()==0) {
			return null; 
		}
		return val;
	}
	public String getMovieTitle(String tconst){
		String val=movieMap.get(tconst);
		if (val.split("\t")[0]==null)
			return null;
		return val.split("\t")[0];
	}
	public String getMovieReleaseYear(String tconst){
		String val=movieMap.get(tconst);
		if (val.split("\t")[1]==null)
			return null;
		return val.split("\t")[1];
	}
	public String getMovieGenres(String tconst){
		String val=movieMap.get(tconst);
		if (val.split("\t")[2]==null)
			return null;
		return val.split("\t")[2];
	}
	public Map<String, String> getMovieMap() {
		return Collections.unmodifiableMap(movieMap);
	}
}