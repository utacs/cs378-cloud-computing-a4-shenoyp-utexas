package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HourOfGpsErrorReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
  @Override
  protected void reduce(IntWritable hour, Iterable<IntWritable> counters, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable counter : counters) {
      sum += counter.get();
    }
    context.write(hour, new IntWritable(sum));
  }
}
