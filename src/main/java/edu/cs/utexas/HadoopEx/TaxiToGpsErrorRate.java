package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiToGpsErrorRate extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("k", 5);
    System.exit(ToolRunner.run(conf, new TaxiToGpsErrorRate(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("k", 5);

    Job job1 = new Job(conf, "TaxiToGpsErrorRate");
    job1.setJarByClass(TaxiToGpsErrorRate.class);

    job1.setMapperClass(TaxiToGpsErrorRateMapper.class);
    job1.setReducerClass(TaxiToGpsErrorRateReducer.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    job1.setInputFormatClass(TextInputFormat.class);

    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.setOutputFormatClass(TextOutputFormat.class);

    if (!job1.waitForCompletion(true)) {
      return 1;
    }

    Job job2 = new Job(conf, "Top5TaxiGpsErrorRates");
    job2.setJarByClass(TaxiToGpsErrorRate.class);

    job2.setMapperClass(TopKTaxisMapper.class);
    job2.setReducerClass(TopKTaxisReducer.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(FloatWritable.class);

    job2.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    job2.setInputFormatClass(KeyValueTextInputFormat.class);

    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    job2.setOutputFormatClass(TextOutputFormat.class);

    return (job2.waitForCompletion(true) ? 0 : 1);
  }
}
