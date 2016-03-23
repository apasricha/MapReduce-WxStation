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

/* Counts up every station to the East and West
 * of the longitude -68167 ie 68.167 West.
 *
 *
 */
public class LandR {
    public static class LandRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {	
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int modstn = Integer.parseInt(line.substring(4, 10));
			//Mappers will go to one of two reducers depending on whether they are West or East
			//of a hard coded longitude. Won't send at all if the longitude isn't valid
			int key2 = 0;
			boolean write = false;
			int longitude = 0;
			if (line.charAt(34) == '+') { // parseInt doesn't like leading plus
				longitude = Integer.parseInt(line.substring(35, 41));
			} else {
				longitude = Integer.parseInt(line.substring(34, 41));
			}
			//if to west, send to one reducer
			if ((longitude < -68167) && (longitude > -180000)) {
				key2 = 0;
				write = true;
			}
			//if to east, send to other
			if ((longitude < 180000) && (longitude > -68167)) {
				key2 = 1;
				write = true;
			}
			//keying on reducer number, value is station no. 
			String keys = Integer.toString(key2);
			if(write) context.write(new Text(keys), new IntWritable(modstn));

		}	
    } // class

    public static class LandRReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//one-to-one hashing in a boolean array, also size is 1 mn because max station # possible is 999999. 
			boolean[] seen = new boolean[1000000];
			int cntr = 0;
			
			//Goes through all the values (station numbers) and marks them as seen or unseen in boolean[] seen
			//If the station hasn't been marked as seen yet, increment a coutner by 1
			for (IntWritable value : values) {
				//converting intwritable to int
				String value1 = value + "";
				if(!seen[Integer.parseInt(value1)]) cntr++;
				//doesn't really make a difference if next line is inside the if or not, true can always be re-marked as true. 
				seen[Integer.parseInt(value1)] = true;
			}
			
			//Return that count. IMP: Note that reducer 0 might hold the east stations, or the west - because the value of the key doesn't appear to
			//make a difference. It is the fact that the keys are -different- that decides everything. So we might have the east stations count in 
			//reducer 0 or in 1, but the west count will always be in the other reducer, never in the same. 
			context.write(key, new IntWritable(cntr));
		}
    } // class


    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "max LandR");
		job.setJarByClass(LandR.class);
		job.setMapperClass(LandRMapper.class);
		//job.setCombinerClass(MaxTemperatureMapper.class);
		job.setReducerClass(LandRReducer.class);
		job.setNumReduceTasks(2); // Set number of reducers
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //main
    
} // class
