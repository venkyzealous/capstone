package analysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
* Map each input stream line to song record of count=1.
* 
* 
* Output
* key = song id 
* values = a song record object with count 1 for each input line
* 
* @author  Venkatesh Jagannathan
* @version 1.0
* @since   2018-11-10 
*/
public class SongTrendMapper extends Mapper<LongWritable,Text,Text,SongRecord> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException{
		try{
			String[] data =  value.toString().split(",");
			
			String date = data[4];
			String hour = data[3];
			
			if(date == null || hour == null) return;
			
			date = date.substring(8);
			
			//skip current & future records after window as we need only historical data
			int day = Integer.parseInt(date);
			if(day >= SongRecord.DayEnd) return; 
			
			Text outputkey = new Text(data[0]);
			SongRecord outputvalue = new SongRecord(date,hour,1);
			
			context.write(outputkey,outputvalue);  //Frequency 1 for each stream
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}
