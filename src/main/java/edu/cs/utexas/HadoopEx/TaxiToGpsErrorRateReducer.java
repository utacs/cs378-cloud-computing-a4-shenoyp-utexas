package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaxiToGpsErrorRateReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
  @Override
  protected void reduce(Text taxi, Iterable<FloatWritable> counters, Context context)
      throws IOException, InterruptedException {
    float gpsErrorCountSum = 0;
    float totalLines = 0;
    for (FloatWritable counter : counters) {
      gpsErrorCountSum += counter.get();
      totalLines += 1;
    }
    context.write(taxi, new FloatWritable(gpsErrorCountSum / totalLines));
  }
}
