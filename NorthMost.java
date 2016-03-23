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


public class NorthMost {
	//Used to find Northernmost station East of the station with the highest elevation.

    public static class NorthMostMapper extends Mapper<LongWritable, Text, Text, LongWritable> {	
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int stn = Integer.parseInt(line.substring(4, 10));	
			int modstn1 = stn % 7; //We are using 7 reducers, keying on station number.
			String modstn = Integer.toString(modstn1);
			int latitude = 0;
			
			//there's definitely something north of the equator, don't need negative latitudes. Those readings will not go to reducers
			
			if (line.charAt(28) == '+') { 
				latitude = Integer.parseInt(line.substring(29, 34));
				int longitude = 0;
				if (line.charAt(34) == '+') { 				
					longitude = Integer.parseInt(line.substring(35, 41));
				} else {
					longitude = Integer.parseInt(line.substring(34, 41));
				}
				
				//The mappers will only go to reducers if the station is located to the east, and latitude is valid. 
				if ((longitude > -68167) && (longitude < 180000) && (latitude <= 90000)) {
					String lat = Integer.toString(latitude);
					String station = Integer.toString(stn);
					String keying = station + "" + lat;
					//11 digit number
					long mix = Long.parseLong(keying);
					//We concatenate the station number and the latitude into the value we send off. We'll 'unpack' it in the reducer
					context.write(new Text(modstn), new LongWritable(mix));
				}
			}
		}	
    } // class

    public static class NorthMostReducer extends Reducer<Text, LongWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			int stat1 = Integer.MIN_VALUE;
			//Goes through all the values and extracts the latitude and station number. 
			for (LongWritable value : values) {
				String value1 = value + "";
				long val1 = Long.parseLong(value1);
				//unpack latitude
				int lat = (int)(val1 % 100000);
				//Keeps track of the highest latitude this reducer has yet seen
				if(maxValue < lat) {
					maxValue = lat;
					//unpack stn no
					stat1 = (int)(val1 / 100000);
				}					
			}
			
			//Outputs the station number and the highest values seen yet. Then it becomes a quesition of find the highest station given out by each reducer
			String stat = Integer.toString(stat1);
			context.write(new Text(stat), new IntWritable(maxValue));
		}
    } // class


    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "max NorthMost");
		job.setJarByClass(NorthMost.class);
		job.setMapperClass(NorthMostMapper.class);
		//job.setCombinerClass(MaxTemperatureMapper.class);
		job.setReducerClass(NorthMostReducer.class);
		job.setNumReduceTasks(7); // Set number of reducers
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class); //Changed from Int Writable
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //main
    
} // class
