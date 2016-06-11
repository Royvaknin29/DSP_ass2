package drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import mappers.SecondMapper;
import mappers.WordCountMapper;
import reducers.SecondReducer;
import writable.SecondReduceOutput;
import writable.SecondSortComperator;
import writable.SeconderySortWritable;
import writable.WordsInDecadeWritable;

public class WordCountTest {
	public static final String HDFS_STOPWORD_LIST = "/data/stopWords.txt";
	public static final String STOPWORD_LIST = "stopWords.txt";

	public static void main(String[] args) throws Exception {
		System.out.println("hi! =]");
		Job firstJob = initFirstJob(args[0], args[1] + "tmp");
		firstJob.waitForCompletion(true);
		Job secondJob = initSecondJob(args[1] + "tmp", args[1]);
		System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
	}

	public static Job initSecondJob(String in, String out) throws IllegalArgumentException, IOException {
		System.out.println("initializing second job..");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "Word Count2");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(SecondMapper.class);
		// job.setCombinerClass(SecondReducer.class);
		// job.setPartitionerClass(SecondPartitioner.class);
		job.setReducerClass(SecondReducer.class);
		job.setMapOutputKeyClass(WordsInDecadeWritable.class);
		job.setMapOutputValueClass(SeconderySortWritable.class);
		job.setSortComparatorClass(SecondSortComperator.class);
		job.setOutputKeyClass(WordsInDecadeWritable.class);
		job.setOutputValueClass(SecondReduceOutput.class);

		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		return job;
	}

	public static Job initFirstJob(String in, String out) throws IllegalArgumentException, IOException {
		System.out.println("initializing first job..");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(HDFS_STOPWORD_LIST);
		fs.copyFromLocalFile(false, true, new Path(STOPWORD_LIST), hdfsPath);
		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
		Job job = new Job(conf, "Word Count");
		job.setJarByClass(WordCountTest.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setMapOutputKeyClass(WordsInDecadeWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(WordsInDecadeWritable.class);
		job.setOutputValueClass(LongWritable.class);
		// job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		return job;
	}
}