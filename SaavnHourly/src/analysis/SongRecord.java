package analysis;

import org.apache.commons.math3.fitting.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;



/**
* This class represents each record of a song and its related data. It also includes global configuration parameters
* for song record processing.
* 
* @author  Venkatesh Jagannathan
* @version 1.0
* @since   2018-11-10 
*/
public class SongRecord implements WritableComparable<SongRecord> {
	
	static int TimeWindowUnits = 100; //100 units = 1 day 
	static int ChartLength = 100;
	static int DayStart = 25; 
	static int DayEnd = 31;  


	IntWritable day;
	IntWritable hour;
	IntWritable frequency;
	IntWritable daytime;
	
	public SongRecord(){
		this.day = new IntWritable();
		this.frequency = new IntWritable();
		this.hour = new IntWritable();
		this.daytime = new IntWritable();
	}
	
	public SongRecord(String day,String hour, int frequency) throws NumberFormatException {
		this.day = new IntWritable(Integer.parseInt(day));
		this.hour = new IntWritable(Integer.parseInt(hour));
		this.frequency = new IntWritable(frequency);
		getDayTimeFromDayHour();
	}
	public SongRecord(int day,int hour, int frequency)  {
		this.day = new IntWritable(day);
		this.frequency = new IntWritable(frequency);
		this.hour = new IntWritable(hour);
		getDayTimeFromDayHour();
	}
	public SongRecord(int daytime, int frequency)  {
		this.daytime = new IntWritable(daytime);
		this.frequency = new IntWritable(frequency);
		this.hour = new IntWritable(0);
		this.day = new IntWritable(0);
		
	}
	public int getDayTimeFromDayHour(int day, int hour){
		if(hour >= 0 && hour <=6)
			return day * 100;
		else if (hour > 6 && hour <= 12)
			return (day * 100) + 25;
		else if (hour > 12 & hour <= 18)
			return (day * 100) + 50;
		else return (day * 100) + 75;
	}
	public IntWritable getDayTimeFromDayHour(){
		return this.daytime =  new IntWritable(
					getDayTimeFromDayHour(this.day.get(), this.hour.get())
				);
	}
	public int getDayTime(){
		return this.daytime.get();
	}
	public int getFrequency(){
		return this.frequency.get() ;
	}
	public int getDay(){
		return this.day.get();
	}
	public int getHour(){
		return this.hour.get();
	}
	public int getDayConverted(){
		return (this.daytime.get()/100)*100;
	}
	public int getHourConverted(){
		return getDayTimeFromDayHour(0,this.hour.get());
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		this.day.readFields(input);
		this.hour.readFields(input);
		this.daytime.readFields(input);
		this.frequency.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		this.day.write(output);
		this.hour.write(output);
		this.daytime.write(output);
		this.frequency.write(output);
	}

	@Override
	public int compareTo(SongRecord record) {
		int cmp = this.day.compareTo(record.day);
		if(cmp!= 0) return cmp;
		else{
			int cmp2 = this.hour.compareTo(record.hour);
			if(cmp2 != 0) return cmp2;
			else
				return this.frequency.compareTo(record.frequency);
		}
		
			
	}
	
	@Override
	public String toString(){
		return this.daytime.get() + "[" + this.frequency.toString() + "]";
	}
	
	 @Override
	public boolean equals(Object o)
	{
	    if(o instanceof SongRecord)
	    {
	    	SongRecord record = (SongRecord) o;
	        return this.day.equals(record.day) && this.hour.equals(record.hour) &&  this.frequency.equals(record.frequency);
	    }
	    return false;
	}
	 
	 @Override
	 public int hashCode(){
		 return (this.day.hashCode() + this.hour.hashCode() + this.frequency.hashCode())/3 ;
	 }

	 private static int getRecencyFactor(int currentDayConverted, int refDayConverted){
		 //give more weight to recent data points
		 int ratio = (currentDayConverted -(refDayConverted-TimeWindowUnits))/(TimeWindowUnits);
		 if(ratio <= 0.4) return 1;
		 else if (ratio <= 0.6) return 4;
		 else if (ratio <= 0.8) return 8;
		 else return 16;
		 
	 }
	 
	 public static double getRoundedAbsWeight(double weight){
		 weight = (weight < 0)?0-weight:weight;
		 weight = ((double)Math.round(weight * 10))/10;
		 return weight;
	 }
	 
	 public static double getTrend(Iterable<SongRecord> songRecords,int refDayConverted, double mean, double stdev){
	 
		 final WeightedObservedPoints obs = new WeightedObservedPoints();
		 for(SongRecord record : songRecords){
			 int currentDayConverted = record.getDayConverted();
			 double delta = (record.getFrequency() - mean); //delta
			 if(delta == 0) continue;
			 double weight = (stdev/delta) * getRecencyFactor(currentDayConverted,refDayConverted) ; //weighted inversely to z-score & proportional to recency  
			 weight = getRoundedAbsWeight(weight);
			 if(weight <= 0 || record.getFrequency() <= 0 || currentDayConverted <= 0 ) continue;
			 obs.add(weight,currentDayConverted, record.getFrequency());
		 }
	 
		 
		 final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(1); //weighted regression line
		 final double[] coeff = fitter.fit(obs.toList()); //get slope of regression line
		 return coeff[1];


		 
	 }

	 public static ArrayList<SongRecord> filterByTimeWindow(Iterable<SongRecord> songRecords,int refDayConverted){
		 
		 ArrayList<SongRecord> windowFiltered = new ArrayList<>();
		 
		 int max = refDayConverted;
		 int min = refDayConverted-TimeWindowUnits;
		 //Include only records that fall within time window
		 for(SongRecord record : songRecords){
			 int current = record.getDayConverted();
			 if( min <= current && current  < max)
				 windowFiltered.add(record);
		 }
		 return windowFiltered;
	 }
}
