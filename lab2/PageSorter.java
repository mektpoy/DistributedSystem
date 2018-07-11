import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageSorter {
	public static class PageMapper extends Mapper <Object, Text, DoubleWritable, Text> {
		private DoubleWritable outputKey = new DoubleWritable();
		private Text outputVal = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			int index = str.indexOf("\t");
			
			String Page = str.substring(0, index);
			String Rank;

			int pos = str.indexOf(":");
			if (pos != -1) {
				Rank = str.substring(index + 1, pos);
			} else {
				Rank = "1.0";
			}
			outputKey.set(-Double.valueOf(Rank));
			outputVal.set(Page);
			context.write(outputKey, outputVal);
		}
	}
	
	public static class PageReducer extends Reducer<DoubleWritable, Text, Text, Text> {
		
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Double pageRank = -key.get();
	  		for (Text value : values) {
	  			context.write(value, new Text(pageRank.toString()));
	  		}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: PageSorter <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Page Sorter");
		job.setJarByClass(PageSorter.class);
		job.setMapperClass(PageMapper.class);
		job.setReducerClass(PageReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("pagerank" + otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("pagerank" + otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}