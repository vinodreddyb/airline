package com.vinod.hadoop.airline.ss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** 
 * SecondarySortDriver is driver class for submitting secondary sort job to Hadoop.
 * This will display each airport departures by departure time
 *
 * @author Vinod
 *
 */
public class AirPortDepartureDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			
			throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
		}
		int returnStatus = submitJob(args);
		System.exit(returnStatus);
	}
	
	public static int submitJob(String[] args) throws Exception {
	
		int returnStatus = ToolRunner.run(new AirPortDepartureDriver(), args);
		return returnStatus;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "AirPort Departures");//new Job(conf, "Secondary Sort");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
           System.err.println("Usage: AirPort Departures Driver <input> <output>");
           System.exit(1);
        }        
       
	    job.setJarByClass(AirPortDepartureDriver.class);
	    job.setJarByClass(AirPortDepartureMapper.class);
	    job.setJarByClass(AirPortDepartureReducer.class);
      
       // set mapper and reducer
	    job.setMapperClass(AirPortDepartureMapper.class);
	    job.setReducerClass(AirPortDepartureReducer.class);
	    
        // define mapper's output key-value
        job.setMapOutputKeyClass(AirPortDepartureCompositKey.class);
        job.setMapOutputValueClass(AirPortDepartureNaturalValue.class);
              
        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // the following 3 setting are needed for "secondary sorting"
        // Partitioner decides which mapper output goes to which reducer 
        // based on mapper output key. In general, different key is in 
        // different group (Iterator at the reducer side). But sometimes, 
        // we want different key in the same group. This is the time for 
        // Output Value Grouping Comparator, which is used to group mapper 
        // output (similar to group by condition in SQL).  The Output Key 
        // Comparator is used during sort stage for the mapper output key.
	    job.setPartitionerClass(AirPortDepartureNaturalKeyPartitioner.class);
	    job.setGroupingComparatorClass(AirPortDepartureNaturalKeyGroupingComparator.class);
	    job.setSortComparatorClass(AirPortDepartureCompositeKeyComparator.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    
	    boolean status = job.waitForCompletion(true);
		
		return status ? 0 : 1;
	}
}
