package analysis;

/**
* This class contains trend information for a song.
* 
* 
* @author  Venkatesh Jagannathan
* @version 1.0
* @since   2018-11-10 
*/
public class SongTrend implements Comparable<SongTrend>  {
	String song;
	double trend;
	
	public SongTrend(String song,double trend){
		this.song = song;
		this.trend = trend;
	}

	public double getTrend(){
		return this.trend;
	}
	
	@Override
	public String toString(){
		return this.song + " " + this.trend;
	}
	
	@Override
	public boolean equals(Object trend){
		if(trend instanceof SongTrend){
			SongTrend current = (SongTrend)trend;
			return ((this.song.equals(current.song)) && (((Double)this.trend).equals(current.trend)));
		}
		else
			return false;
	}

	@Override
	public int compareTo(SongTrend songTrend) {
		return ((Double)this.getTrend()).compareTo((Double)songTrend.getTrend()); 
	}

}
