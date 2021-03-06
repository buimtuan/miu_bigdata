package bd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

public class ApacheLogInMapper extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, PairWritable<DoubleWritable, IntWritable>> {

		private Map<String, Pair<Double, Integer>> map;

		private final String regex = "^(\\S+) (\\S+) (\\S+) " +
               "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +
               " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)";
		private final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

		@Override
		protected void setup(Context ctx)  {
			map = new HashMap<String, Pair<Double, Integer>>();
		}
		public void map(LongWritable key, Text value, Context ctx) {

			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				try {
					String ip = matcher.group(1);
					double q = Double.parseDouble(matcher.group(9));
					if (map.containsKey(ip)) {
						Pair<Double, Integer> d = map.get(ip);
						d.setLeft(d.getLeft() + q);
						d.setRight(d.getRight() + 1);
						map.put(ip, d);
					} else {
						map.put(ip, new Pair<>(q, 1));
					}
				} catch (NumberFormatException  ex) {
				}
			}
		}

		@Override
		protected void cleanup(Context ctx) throws IOException, InterruptedException {
			for (String k : map.keySet()) {
				Pair<Double, Integer> d = map.get(k);
				ctx.write(new Text(k), new PairWritable<DoubleWritable, IntWritable>(
					new DoubleWritable(d.getLeft()), new IntWritable(d.getRight())));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, PairWritable<DoubleWritable, IntWritable>, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<PairWritable<DoubleWritable, IntWritable>> values, Context ctx)
				throws IOException, InterruptedException {
			double sum = 0.0;
			double cnt = 0.0;
			for (PairWritable<DoubleWritable, IntWritable> val : values) {
				sum += val.getLeft().get();
				cnt += val.getRight().get();
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
