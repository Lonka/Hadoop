package lonka.test.example;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMR {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://172.16.22.146:8000");
		conf.set("mapred.job.tracker", "172.16.22.146:8001");
		//conf.set("hadoop.job.ugi", "hadoop");
		//conf.set("Hadoop.tmp.dir", "/user/gqshao/temp/");
		
		Job job = Job.getInstance(conf, "LonkaTestWordCount");
		job.setJarByClass(lonka.test.example.WordCountMR.class);
		// TODO: specify a mapper
		job.setMapperClass(WordCountMp.class);
		// TODO: specify a reducer
		job.setReducerClass(WordCountRd.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//job.setInputFormatClass(TextInputFormat.class);
		// job.setInputFormatClass(TextOutputFormat.class);
		// TODO: specify input and output DIRECTORIES (not files)
		
		//inputpath可以多個
		FileInputFormat.setInputPaths(job, new Path(
				"hdfs://172.16.22.146:8000/user/lonka.liu/input02"));
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://172.16.22.146:8000"),conf);
		Path outputPath = new Path("hdfs://172.16.22.146:8000/user/lonka.liu/output02");
		if(hdfs.exists(outputPath))
		{
			hdfs.delete(outputPath);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(true))
			return;
	}

}
