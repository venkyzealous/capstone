package analysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
* Entry point class for song record processing. 
* Sets Mapper, Combiner, Reducer & Formats
* Number of Reduce = 1
* 
* 
* @author  Venkatesh Jagannathan
* @version 1.0
* @since   2018-11-10 
*/
public class SongTrendDriver  extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		
	        int returnStatus = ToolRunner.run(new Configuration(), new SongTrendDriver(), args);

	        System.exit(returnStatus);
	    }
	
	public int run(String[] args) throws IOException{
   
		
    	Job job = new Job(getConf());
    	
    	
    	 job.setJobName("SongTrend");
    	
    	 job.setJarByClass(SongTrendDriver.class);
    	
    	
    	 
    	 job.setMapperClass(SongTrendMapper.class);
    	 job.setCombinerClass(SongTrendCombiner.class);
    	 job.setReducerClass(SongTrendReducer.class);
    	
    	 job.setOutputKeyClass(IntWritable.class);
    	 job.setOutputValueClass(Text.class);
    	 job.setMapOutputKeyClass(Text.class);
    	 job.setMapOutputValueClass(SongRecord.class);
    	 
    	 job.setNumReduceTasks(1);
    	 job.setInputFormatClass(TextInputFormat.class);

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job,new Path(args[1]));
    	   	
    	
    	try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 0;
      
    }

}
