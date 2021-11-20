package analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* Reduces song wise combined inputs to list if trending songs as per configuration in SongRecord class 
* Filters record by window, calculates trend & uses priorityqueue to hold top songs
* 
* Input 
* key = song id
* values = collection of song records with combined count
* 
* Output
* key = day number
* values = trending song in trend order
* 
* @author  Venkatesh Jagannathan
* @version 1.0
* @since   2018-11-10 
*/
public class SongTrendReducer extends Reducer<Text,SongRecord,IntWritable,Text> {
	
	
	HashMap<Integer,PriorityQueue<SongTrend>> queues = new HashMap<>();
	
	
	@Override
	public void reduce(Text key, Iterable<SongRecord> values,Context context) throws InterruptedException,IOException{

		

		HashMap<Integer,Integer> map = new HashMap<>();
		
		for(SongRecord value:values){   //Reduce data across mappers
			
			int daytime = value.getDayTime();
			
			if(map.containsKey(daytime))
				map.put(daytime, map.get(daytime)+value.getFrequency()); //aggregate song frequency
			else
				map.put(daytime, value.getFrequency() );
		}
		
		ArrayList<SongRecord> accumulated = new ArrayList<SongRecord>();
		Iterator<Entry<Integer,Integer>> iterator = map.entrySet().iterator();
		
		DescriptiveStatistics ds = new DescriptiveStatistics();
		while(iterator.hasNext()){
			Entry<Integer,Integer> entry = iterator.next();
			accumulated.add(new SongRecord(entry.getKey(),entry.getValue()));
			ds.addValue(entry.getValue());
			
		}
		//Calculate standard deviation on whole data
		double stdev = ds.getStandardDeviation();
		stdev = ((double)Math.round(stdev * 10))/10;
		double mod = (stdev < 0)?0-stdev:stdev;
		
		double mean = ds.getMean();
		
		
		for(int day = SongRecord.DayStart; day <= SongRecord.DayEnd; day++){  // identify trend for each song in order
			PriorityQueue<SongTrend> queue;
			if(queues.containsKey(day))
				queue = queues.get(day);
			else{
				queue = new PriorityQueue<SongTrend>(); //day wise queue
				queues.put(day, queue);
			}
			int dayConverted = day * 100;
			
			ArrayList<SongRecord> windowFiltered = SongRecord.filterByTimeWindow(accumulated, dayConverted); 
			
			
			if(windowFiltered.size() <= 2) return; //insufficient data
			
			//calculate increasing trend in window & weight by overall absolute coefficient of variance to push down
			//old songs which should have less variance
			try{
				double trend = SongRecord.getTrend(windowFiltered,dayConverted,mean,stdev) * (mod);
				queue.add(new SongTrend(key.toString(),trend));
			}
			catch(MathIllegalArgumentException e){
				System.out.println(e.getMessage());
				continue;
			}
			if(queue.size() > SongRecord.ChartLength)
				while(queue.size() > SongRecord.ChartLength)
					queue.poll(); //evict least trend song from queue to limit to chart length
			
		}

	}
	
	@Override
	public void cleanup(Context context) throws InterruptedException,IOException{
		Iterator<Entry<Integer,PriorityQueue<SongTrend>>> iterator = this.queues.entrySet().iterator();
		
		while(iterator.hasNext()){
			Entry<Integer,PriorityQueue<SongTrend>> entry = iterator.next(); 
			Integer day = entry.getKey();
			PriorityQueue<SongTrend> queue = entry.getValue();
			ArrayList<SongTrend> songs = new ArrayList<>();

			while(!queue.isEmpty())
				songs.add(queue.poll());
			
			ListIterator<SongTrend> queueIterator = songs.listIterator(songs.size());
			while(queueIterator.hasPrevious()){
				SongTrend current = queueIterator.previous();
				context.write(new IntWritable(day), new Text(current.song)); //write output in reverse order
			}
		
		}
		
	}

}
