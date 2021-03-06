import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GraphBuilder {
	
	public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputVal = new Text();
		private static final String TitleRegex = "<title>.*</title>";
		private static final String LinkRegex = "\\[\\[[^\\[\\]]*\\]\\]";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String content = value.toString();
			Pattern TitlePattern = Pattern.compile(TitleRegex);
			Matcher TitleMatcher = TitlePattern.matcher(content);
			if (TitleMatcher.find())
			{
				String title = TitleMatcher.group().replaceAll("<title>|</title>", "");
				outputKey.set(title);
			}
			else
			{
				return;
			}

			Pattern LinkPattern = Pattern.compile(LinkRegex);
			Matcher LinkMatcher = LinkPattern.matcher(content);
			while (LinkMatcher.find())
			{
				String s = LinkMatcher.group();
				String link = s.substring(2, s.length() - 2);
				int pos = link.indexOf("|");
				if (pos != -1) {
					outputVal.set(link.substring(0, pos));
				} else {
					outputVal.set(link);
				}
				context.write(outputKey, outputVal);
			}
		}
	}
	
	public static class GraphBuilderReducer extends Reducer<Text,Text,Text,Text> {
		private Text outputKey = new Text();
		private Text outputVal = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			outputKey.set(key);
			StringBuilder sBuilder = new StringBuilder();
			boolean first = true;
			for (Text inputVal : values) {
				String doc = inputVal.toString();
				if (!first) {
					sBuilder.append("^");
				} else {
					first = false;
				}
				sBuilder.append(doc);
			}
			outputVal.set(sBuilder.toString());
			context.write(outputKey, outputVal);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: GraphBuilder <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Graph Builder");
		job.setJarByClass(GraphBuilder.class);
		job.setMapperClass(GraphBuilderMapper.class);
		job.setReducerClass(GraphBuilderReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
