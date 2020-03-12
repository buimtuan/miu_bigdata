package bd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

public class Part3 extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, ArrayOfMap> {

		private Map<String, List<Map<String, Double>>> map;

		@Override
		protected void setup(Context ctx) {
			map = new HashMap<>();
		}

		public void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] items = line.split(" ");

			for (int i = 0; i < items.length; ++i) {
				Map<String, Double> m = new HashMap<>();
				for (int j = i + 1; j < items.length; ++j) {
					if (items[j].equals(items[i])) {
						break;
					}
					double v = 1.0;
					if (m.containsKey(items[j])) {
						double tmp = m.get(items[j]);
						v = v + tmp;
					}
					m.put(items[j], v);
				}
				if (map.containsKey(items[i])) {
					List<Map<String, Double>> tmp = map.get(items[i]);
					tmp.add(m);
					map.put(items[i], tmp);
				} else {
					List<Map<String, Double>> tmp = new ArrayList<>();
					tmp.add(m);
					map.put(items[i], tmp);
				}
			}

		}

		@Override
		protected void cleanup(Context ctx)
				throws IOException, InterruptedException {
			for (String k : map.keySet()) {
				ctx.write(new Text(k), ArrayOfMap.of(map.get(k)));
			}
		}

	}

	public static class MyReducer extends Reducer<Text, ArrayOfMap, Text, MyMap> {

		public void reduce(Text key, Iterable<ArrayOfMap> values, Context ctx)
				throws IOException, InterruptedException {
			double sum = 0;
			Map<String, Double> m = new HashMap<>();
			for (ArrayOfMap val : values) {
				List<MyMap> myMapList = val.toList();
				for (MyMap myMap : myMapList) {
					for (Writable wk : myMap.keySet()) {
						String k = ((Text)wk).toString();
						Double v = ((DoubleWritable)myMap.get(wk)).get();
						sum = sum + v;
						if (m.containsKey(k)) {
							Double tmp = m.get(k);
							v = v + tmp;
						}
						m.put(k, v);
					}
				}
			}
			MyMap map = new MyMap();
			for (String k : m.keySet()) {
				map.put(k, m.get(k) / sum);
			}
			ctx.write(key, map);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(Part3.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayOfMap.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyMap.class);

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
		int res = ToolRunner.run(conf, new Part3(), args);
		System.exit(res);
	}

}
