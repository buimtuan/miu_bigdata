package bd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

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
import lombok.extern.log4j.Log4j;

@Log4j
public class WordCount extends Configured implements Tool {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Map<String, Integer> map;

		@Override
		protected void setup(Context context) {
			map = new HashMap<String, Integer>();
		}

		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			Integer sum;
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				if (map.containsKey(word)) {
					sum = map.get(word);
					sum++;
				} else {
					sum = 1;
				}
				map.put(word, sum);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			var word = new Text();
			var sum = new IntWritable();
			for (String k : map.keySet()) {
				word.set(k);
				sum.set(map.get(k));
				context.write(word, sum);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			log.info("reducer run");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new WordCount(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(WordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}