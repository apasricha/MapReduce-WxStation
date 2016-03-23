import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/* This just looks for the station at elevation 4080 (found with the MaxElevation program) 
 * The Max Value operation in the reducer is completely unnecessary, leftover from MaxElevation 
 * but still gave us the output we needed
 *
 */
 
public class FindMaxElevation {
    public static class MaxElevationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final int MISSING = 9999;		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			//keying on modstn, i.e., station number
			String modstn = Integer.toString(((Integer.parseInt(line.substring(4, 10)))));	
			int elevation = (Integer.parseInt(line.substring(47, 51)));
			//send to reducer if found station
			if (elevation == 4080) {
				int longitude = 0;
				if (line.charAt(34) == '+') { // parseInt doesn't like leading plus
				// signs
					longitude = Integer.parseInt(line.substring(35, 41));
				} else {
					longitude = Integer.parseInt(line.substring(34, 41));
				}						
				context.write(new Text(modstn), new IntWritable(longitude));
			}			
		}	
    } // class

    public static class MaxElevationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			//We should have just returned any of the key-value pairs from the what was given to the reducer. Unsure of how value.get() would
			//work we decided to not rock the boat and keep this loop which returns what we need anyway
		
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			// key is stn no, value is long
			context.write(key, new IntWritable(maxValue));
		}
    } // class


    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "max elevation");
		job.setJarByClass(FindMaxElevation.class);
		job.setMapperClass(MaxElevationMapper.class);
		//job.setCombinerClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxElevationReducer.class);
		job.setNumReduceTasks(1); // Set number of reducers. Need just 1 for only 1 station
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //main
    
} // class
