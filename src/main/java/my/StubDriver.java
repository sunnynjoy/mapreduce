package my;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static org.apache.hadoop.mapreduce.Job.getInstance;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath;
import static org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat.setRecordLength;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath;

public class StubDriver {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		JobConf conf = new JobConf();
		Job job = getInstance(conf);
		job.setJarByClass(StubDriver.class);
		
		job.setMapperClass(StubMapper.class);
		job.setReducerClass(StubReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		setRecordLength(conf, 15);
		
		addInputPath(job, new Path(args[0]));
		setOutputPath(job, new Path(args[1]));
		
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
