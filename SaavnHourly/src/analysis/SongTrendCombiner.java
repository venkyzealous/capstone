package analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* This is combiner class for song record data. It reduces network load by aggregating
* stream data of each day of input song. 
* 
* Input 
* key = song id
* values = collection of song records 1 per stream record
* 
* Output
* key = song id 
* values = collection of song records grouped by day aggregated on frequency
* 
* @author  Venkatesh Jagannathan
* @version 1.0
* @since   2018-11-10 
*/
public class SongTrendCombiner extends Reducer<Text,SongRecord,Text,SongRecord> {
	@Override
	public void reduce(Text key, Iterable<SongRecord> values,Context context) throws InterruptedException,IOException{

	

		HashMap<Integer,Integer> map = new HashMap<>();
		
		for(SongRecord value:values){   //Aggregate daytime wise data

			int daytime = value.getDayTime();
			
			if(map.containsKey(daytime))
				map.put(daytime, map.get(daytime)+1); //Increment per input record
			else
				map.put(daytime, 1);
			
			
		}
		
		Iterator<Entry<Integer,Integer>> iterator = map.entrySet().iterator();
		while(iterator.hasNext()){
			
			Entry<Integer,Integer> entry = iterator.next();
			context.write(key, new SongRecord(entry.getKey(),entry.getValue()) ); //Output aggregates
		}
		
	}

}
