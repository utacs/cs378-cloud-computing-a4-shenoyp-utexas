package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaxiToAvgEarningsReducer extends Reducer<Text, DualFloatWritable, Text, FloatWritable> {
  @Override
  protected void reduce(Text taxi, Iterable<DualFloatWritable> counters, Context context)
      throws IOException, InterruptedException {
    float earningsSum = 0;
    float minutesSum = 0;
    for (DualFloatWritable counter : counters) {
      earningsSum += counter.getFirst();
      minutesSum += counter.getSecond();
    }
    context.write(taxi, new FloatWritable(earningsSum / minutesSum));
  }
}
