package bd;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import lombok.var;

public class ApacheLog extends Configured implements Tool {

	public static class MyMapper
			extends Mapper<LongWritable, Text, Text, DoubleWritable> {

				private final String regex = "^(\\S+) (\\S+) (\\S+) " +
               "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +
               " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)";

			   private final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

		public void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException
		{
			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				try {
					String ip = matcher.group(1);
					var q = Double.parseDouble(matcher.group(9));
					ctx.write(new Text(ip), new DoubleWritable(q));
				} catch (NumberFormatException  ex) {
				}
			}
		}

	}

	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values, Context ctx)
				throws IOException, InterruptedException
		{
			long sum = 0;
			long cnt = 0;
			for (var val : values) {
				sum += val.get();
				cnt += 1.0;
			}
			ctx.write(key, new DoubleWritable(sum/cnt));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(ApacheLog.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Missing parameters");
			return;
		}
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(args[1]), true);
		int res = ToolRunner.run(conf, new ApacheLog(), args);
		System.exit(res);
	}

}
