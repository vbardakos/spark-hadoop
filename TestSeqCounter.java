import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.*;

import mochadoop.Mochadoop;

public class TestSeqCounter
{	
	public static void main(String[] args) throws Exception 
	{		
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.OFF);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sequence Counter Test");
		
		// The following line will add the file name 'short-seq.txt' to the list
		// of cache file names to be used in the mapper. You will
		// need to set the equivalent line in the main method
		// of your actual Map/Reduce main method that will be used on Hadoop but with a 
		// # tag on the end of the file name. See the lecture notes for an example.
	    job.addCacheFile(new URI("seq20.txt#pat"));

		job.setJarByClass(DNASeqCount.class);
		
		// Set mapper class to SeqMapper 
		job.setMapperClass(DNASeqCount.SeqMapper.class);
		
		// Set combine class to SeqCombiner
		// Uncomment the following when you are ready to test your Combiner
		job.setCombinerClass(DNASeqCount.SeqCombiner.class);
		
		// Set reduce class to SeqReducer
		job.setReducerClass(DNASeqCount.SeqReducer.class);
		
		// Class of output key is Text
		job.setOutputKeyClass(Text.class);		
		
		// Class of output value is IntWritable
		job.setOutputValueClass(IntWritable.class);
		
		// Input path is first argument when program called
		FileInputFormat.addInputPath(job, new Path("dna-short.txt"));
		
		// Output path is second argument when program called
		FileOutputFormat.setOutputPath(job, new Path("results_short.txt"));
		
		Mochadoop mh = new Mochadoop();
		// Set the second parameter to true if you want to see the intermediate
		// input/output from the mapper, combiner and reducer. Set it to false
		// when dealing with larger data sets. The third parameter names 
		// a file to save the debug output to. If this parameter is omitted, the debug output
		// will not be saved. Note that you will need to refresh your Eclipse
		// project view (F5) to see any new files that may have been created by your program.
		// mh.setNumMappers(3);
		mh.runMapReduce(job,true);
	}  
}