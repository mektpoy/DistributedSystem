import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRanker {
	public static class PageMapper extends Mapper <Object, Text, Text, Text> {
		private Text outputKey = new Text();
		private Text outputVal = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			int index = str.indexOf("\t");
			
			String Page = str.substring(0, index);
			String Rank;
			String LinkStr = "";

			int pos = str.indexOf(":");
			if (pos != -1) {
				Rank = str.substring(index + 1, pos);
				LinkStr = str.substring(pos + 1);
			} else {
				Rank = "1.0";
				LinkStr = str.substring(index + 1);
			}

			outputKey.set(Page);
			outputVal.set("|" + LinkStr);
			context.write(outputKey, outputVal);

			if (LinkStr == "") {
				return;
			}
			String[] LinkList = LinkStr.split(",");
			double val = Double.valueOf(Rank) / LinkList.length;
			for (String Link : LinkList) {
				outputKey.set(Link);
				outputVal.set(String.valueOf(val));
				context.write(outputKey, outputVal);
			}
		}
	}
	
	public class PageCombiner extends Reducer<Text, Text, Text, Text> {
		private Text outputVal = new Text();

		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			boolean flag = false;
			String str = "";
			double total = 0;

			for (Text value : values) {
				str = value.toString();
				if (str.startsWith("|")) {
					flag = true;
					outputVal.set(str);
					context.write(key, outputVal);
					continue;
				}
				double GetRank = Double.valueOf(str);
				total += GetRank;
			}

			double Rank = total;

			if (!flag) {
				outputVal.set(String.valueOf(Rank));
				context.write(key, outputVal);
				return;
			}
		}
	}

	public static class PageReducer extends Reducer <Text, Text, Text, Text> {
		private Text outputVal = new Text();
		private static final double alpha = 0.15;

		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			boolean flag = false;
			String str = "";
			String LinkStr = "";
			double total = 0;

			for (Text value : values) {
				str = value.toString();
				if (str.startsWith("|")) {
					flag = true;
					LinkStr = str.substring(1);
					continue;
				}
				double GetRank = Double.valueOf(str);
				total += GetRank;
			}

			if (!flag) {
				return;
			}

			double Rank = (1.0 - alpha) * total + alpha;
			outputVal.set(String.valueOf(Rank) + ":" + LinkStr);
			context.write(key, outputVal);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: PageRanker <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Page Ranker");
		job.setNumReduceTasks(10);
		job.setJarByClass(PageRanker.class);
		job.setMapperClass(PageMapper.class);
		job.setReducerClass(PageReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("pagerank" + otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("pagerank" + otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}