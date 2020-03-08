
import org.apache.hadoop.io.Text;

public class MovieMetadataParser {

	private String tconst;
	private String movieTitle;
	private String movieReleaseYear;
	private String movieGenre;

	public boolean parse(String movieRecord) {
		String[] movieRecords = movieRecord.split("\t");
		tconst = movieRecords[0];
		movieTitle = movieRecords[1];
		movieReleaseYear = movieRecords[2];
		movieGenre = movieRecords[3];
		return true;
	}
	public boolean parse(Text movieRecord) {
		return parse(movieRecord.toString());
	}
	public String getMovieID() {
		return tconst;
	}
	public String getMovieReleaseYear() {
		return movieReleaseYear;
	}
	public String getMovieTitle() {
		return movieTitle;
	}
	public String getMovieGenres() {
		return movieGenre;
	}
}

