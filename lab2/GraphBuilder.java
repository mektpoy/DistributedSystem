import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GraphBuilder {

	public class WholeFileInputFormat extends FileInputFormat<Text,Text>{
	    @Override
	    public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
	        RecordReader<Text,Text> recordReader = new WholeFileRecordReader();
	        return recordReader;
	    }
	}

	public class WholeFileRecordReader extends RecordReader<Text,Text>{

	    private FileSplit fileSplit;
	    private JobContext jobContext;
	    private Text currentKey = new Text();
	    private Text currentValue = new Text();
	    private boolean finishConverting = false;

	    @Override
	    public void close() throws IOException {
	    }

	    @Override
	    public Text getCurrentKey() throws IOException, InterruptedException {
	        return currentKey;
	    }

	    @Override
	    public Text getCurrentValue() throws IOException, InterruptedException {
	        return currentValue;
	    }

	    @Override
	    public float getProgress() throws IOException, InterruptedException {
	        float progress = 0;
	        if(finishConverting){
	            progress = 1;
	        }
	        return progress;
	    }

	    @Override
	    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
	        this.fileSplit = (FileSplit) arg0;
	        this.jobContext = arg1;
	        String filename = fileSplit.getPath().getName();
	        this.currentKey = new Text(filename);
	    }

	    @Override
	    public boolean nextKeyValue() throws IOException, InterruptedException {
	        if (!finishConverting) {
	            Path file = fileSplit.getPath();
	            FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
	            FSDataInputStream in = fs.open(file);
	            BufferedReader br = new BufferedReader(new InputStreamReader(in));
	            String line="";
	            String total="";
	            while((line= br.readLine())!= null){
	                total =total+line+"\n";
	            }
	            br.close();
	            in.close();
	            fs.close();
	            currentValue = new Text(total);
	            finishConverting = true;
	            return true;
	        }
	        return false;
	    }

	}
	
	public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputVal = new Text();
		private static final String TitleRegex = "<title>.*</title>";
		private static final String LinkRegex = "\\[\\[[\\[\\]]*]\\]\\]";
			
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
				throw new InterruptedException();
			}

			Pattern LinkPattern = Pattern.compile(LinkRegex);
			Matcher LinkMatcher = LinkPattern.matcher(content);
			while (LinkMatcher.find())
			{
				String s = LinkMatcher.group();
				String link = s.substring(2, s.length() - 4);
				outputVal.set(link);
				context.write(outputKey, outputVal);
			}
		}
	}
	
	public static class GraphBuilderReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
			context.write(key, values);
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
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(GraphBuilderMapper.class);
		job.setCombinerClass(GraphBuilderReducer.class);
		job.setReducerClass(GraphBuilderReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
