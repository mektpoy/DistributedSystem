import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndex {

	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{

		private static final String stopWordPath = "lab1/freword.txt";

		private Text outputKey = new Text();
		private Text outputVal = new Text();
		private static final String REGEX = "\\w+";
		private Set<String> stopWords = new HashSet<String>();
		private FileSystem fs;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = new Configuration();
			fs = FileSystem.get(conf);
			FSDataInputStream dis = fs.open(new Path(stopWordPath));
			while (true) {
				String line = dis.readLine();
				if (line == null || line.isEmpty()) break;
				StringTokenizer tokenizer = new StringTokenizer(line);
				while (tokenizer.hasMoreTokens())
					stopWords.add(tokenizer.nextToken());
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			outputVal.set(fileName);
			String line = value.toString();
			Pattern p = Pattern.compile(REGEX);
			Matcher m = p.matcher(line);
			while (m.find())
			{
				String word = m.group().toLowerCase();
				if (stopWords.contains(word)) continue;
				outputKey.set(word);
				context.write(outputKey, outputVal);
			}
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
		private Text outputKey = new Text();
		private Text outputVal = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> docs = new HashSet<String>();
			outputKey.set(key);
			StringBuilder sBuilder = new StringBuilder();
			boolean first = true;
			for (Text inputVal : values) {
				String doc = inputVal.toString();
				if (docs.contains(doc)) {
					continue;
				} else {
					docs.add(doc);
				}
				if (!first) {
					sBuilder.append(", ");
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
			System.err.println("Usage: InvertedIndex <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Inverted Index");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexReducer.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
