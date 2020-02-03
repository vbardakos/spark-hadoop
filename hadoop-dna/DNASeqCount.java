import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.sound.midi.Sequence;

/**
 * DNASeqCount contains the Mapper, Combiner and Reducer classes. Special thanks
 * to Dr. David Cairns for providing a template to work on.
 *
 * @author Vasileios Bardakos
 *
 */

public class DNASeqCount
{

	// The mapper class
	public static class SeqMapper extends Mapper<Object, Text, Text, TwoIntWritable>
	{
		ArrayList<Position> sequence = new ArrayList<>();
		TwoIntWritable myWritable = new TwoIntWritable();

		/**
	     * Processes a line that is passed to it by writing a key/value pair to the context.
	     *
	     * @param index 	A link to the file index
	     * @param value 	The line from the file
	     * @param context 	The Hadoop context object
	     */
	    public void map(Object index, Text value, Context context) throws IOException, InterruptedException
		{
			Iterator<Position> sequenceIter = sequence.iterator();

			while (sequenceIter.hasNext())
			{
				Position protein = sequenceIter.next();
				protein.setValue(value.toString());

				int start = protein.start(0);
				int end = protein.end(0);

				while (start != -1 && end != -1)
				{
					if (start+2 < end)
					{
						myWritable.setDistance(protein.getDistance());
						context.write(new Text(protein.toString()), myWritable);

						start = protein.start(end+1);
					}
					end = protein.end(end+1);
				}
			}
		}

	    public void setup(Context context)
	    	// Load in any data you need for your mapper here...
		{
			try
			{
				URI[] cacheFiles = context.getCacheFiles();
				String[] fname=cacheFiles[0].toString().split("#");
				// Now use the tag after the # in the original filename as the
				// internal HDFS filename reference.
				BufferedReader br = new BufferedReader(new FileReader(fname[0]));
				String cache_line=br.lines().collect(Collectors.joining(System.lineSeparator()));
				String parts[] = cache_line.split("[\n]");

				String codon[];
				for (String pairs : parts)
				{
					codon = pairs.split(",");
					sequence.add(new Position(codon[0],codon[1]));
				}

				br.close();
			}
			catch (Exception e)
			{
				System.err.println("MyWARNING: " + e);
				System.exit(0);
			}
	    }

	}

	// The Combiner class
	public static class SeqCombiner extends Reducer<Text,TwoIntWritable,Text,TwoIntWritable>
	{
		TwoIntWritable myWritable = new TwoIntWritable();

		public void reduce(Text key, Iterable<TwoIntWritable> values, Context context) throws IOException, InterruptedException
	    {
			int sum = 0;
			int counter = 0;

			for (TwoIntWritable i : values)
			{
				sum += i.getDistance().get();
				counter += i.getCount().get();
			}

			myWritable.setDistance(sum);
			myWritable.setCount(counter);

			context.write(key, myWritable);
		}
	}

	// The Reducer class
	public static class SeqReducer extends Reducer<Text,TwoIntWritable,Text,FloatWritable>
	{

	    public void reduce(Text key, Iterable<TwoIntWritable> values, Context context) throws IOException, InterruptedException
	    {
			int sum = 0;
			int counter = 0;

			for (TwoIntWritable i : values)
			{
				sum += i.getDistance().get();
				counter += i.getCount().get();
			}

			float average = (float) sum/counter;

			context.write(key, new FloatWritable(average));
	    }
	}


//	  /**
//	   * main program that will be run, including configuration setup
//	   *
//	   * @param args		Command line arguments
//	   */
	  public static void main(String[] args) throws Exception
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Sequence Counter");
	    job.setJarByClass(DNASeqCount.class);

	    // Set mapper class to SeqMapper defined above
	    job.setMapperClass(SeqMapper.class);

		// Set combine class to SeqCombiner
		// Uncomment the following when you are ready to test your Combiner
		job.setCombinerClass(SeqCombiner.class);

	    // Set reduce class to IntSumReducer defined above
	    job.setReducerClass(SeqReducer.class);

	    // Class of output key is Text
	    job.setOutputKeyClass(Text.class);

	    // Class of output value is IntWritable
	    job.setOutputValueClass(Text.class);

	    // Input path is first argument when program called
	    FileInputFormat.addInputPath(job, new Path(args[0]));

	    // Output path is second argument when program called
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    // waitForCompletion submits the job and waits for it to complete,
	    // parameter is verbose. Returns true if job succeeds.
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}



