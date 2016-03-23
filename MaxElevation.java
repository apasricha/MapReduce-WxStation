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

//Finds the max elevation of any station. The station no and longitude is found in FindMaxElevation. 

public class MaxElevation {
    public static class MaxElevationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final int MISSING = 9999;		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String modstn = Integer.toString(((Integer.parseInt(line.substring(4, 10)))%5));
			//We are using 5 reducers, so just sending each station over, keying on station number. 
			int elevation = (Integer.parseInt(line.substring(47, 51)));
			if (elevation != MISSING) {			
				context.write(new Text(modstn), new IntWritable(elevation));
			}			
		}	
    } // class

    public static class MaxElevationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//Essentially doing what maxTemp_v2 did. Find the max in all of our values
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
    } // class


    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "max elevation");
		job.setJarByClass(MaxElevation.class);
		job.setMapperClass(MaxElevationMapper.class);
		//job.setCombinerClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxElevationReducer.class);
		job.setNumReduceTasks(5); // Set number of reducers
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //main
    
} // class 
