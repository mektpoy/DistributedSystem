import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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

public class GetTitle {
	
	public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputVal = new Text();
		private static final String TitleRegex = "<title>.*</title>";
		private Set<String> HashList = new HashSet<String>();	

		public String hash(String str) {
		    char s[] = str.toCharArray();
			int len = str.length();
			long ret = 0;
			for (int i = 0; i < len; i ++)
				ret = ret * 31 + s[i];
			return String.valueOf(ret);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			HashList.add("676305997839911778");
			HashList.add("-3951490323760660577");
			HashList.add("5412505450899597930");
			HashList.add("859603407346768615");
			HashList.add("-6683382577635278880");
			HashList.add("64485397417");
			HashList.add("8585460705707051799");
			HashList.add("6631008471983050830");
			HashList.add("2011108078");
			HashList.add("3879212236676474077");
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String content = value.toString();
			Pattern TitlePattern = Pattern.compile(TitleRegex);
			Matcher TitleMatcher = TitlePattern.matcher(content);
			if (TitleMatcher.find())
			{
				String title = TitleMatcher.group().replaceAll("<title>|</title>", "");
				String hashValue = hash(title);
				if (HashList.contains(hashValue)) {
					outputKey.set(hash(title));
					outputVal.set(title);
				}
				else
					return;
			}
			else
			{
				return;
			}
			context.write(outputKey, outputVal);
		}
	}
	
	public static class GraphBuilderReducer extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: GetTitle <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Get Title");
		job.setNumReduceTasks(0);
		job.setJarByClass(GetTitle.class);
		job.setMapperClass(GraphBuilderMapper.class);
		job.setReducerClass(GraphBuilderReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
