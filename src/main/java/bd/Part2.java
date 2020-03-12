package bd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class Part2 extends Configured implements Tool {

	public static class MyMapper
			extends Mapper<LongWritable, Text, TextPair, DoubleWritable> {

		private Map<Pair<String, String>, Double> map;

		@Override
		protected void setup(Context ctx) {
			map = new HashMap<>();
		}

		public void map(LongWritable key, Text value, Context ctx) {

			String line = value.toString();
			String[] items = line.split(" ");

			for (int i = 0; i < items.length; ++i) {
				Pair<String, String> asteriskKey = Pair.of(items[i], "*");
				double sumAsterisk = 0.0;
				for (int j = i + 1; j < items.length; ++j) {
					if (items[j].equals(items[i])) {
						break;
					}
					sumAsterisk++;
					Pair<String, String> pairKey = Pair.of(items[i], items[j]);
					Double sum = 1.0;
					if (map.containsKey(pairKey)) {
						sum += map.get(pairKey);
					}
					map.put(pairKey, sum);
				}
				if (map.containsKey(asteriskKey)) {
					sumAsterisk += map.get(asteriskKey);
				}
				map.put(asteriskKey, sumAsterisk);
			}
		}

		@Override
		protected void cleanup(Context ctx) throws IOException, InterruptedException {
			for (Pair<String, String> k : map.keySet()) {
				ctx.write(TextPair.of(k), new DoubleWritable(map.get(k)));
			}
		}

	}

	public static class MyReducer extends Reducer<TextPair, DoubleWritable, TextPair, DoubleWritable> {

		private double sum;

		protected void setup(Context ctx)
				throws IOException, InterruptedException {
		}

		public void reduce(TextPair key, Iterable<DoubleWritable> values, Context ctx)
				throws IOException, InterruptedException {

			double s = 0.0;
			for (DoubleWritable v : values) {
				s = s + v.get();
			}
			if (key.getRight().toString().equals("*")) {
				sum = s;
				return;
			}
			ctx.write(key, new DoubleWritable(s/sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(Part2.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(TextPair.class);
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
		int res = ToolRunner.run(conf, new Part2(), args);
		System.exit(res);
	}

}
