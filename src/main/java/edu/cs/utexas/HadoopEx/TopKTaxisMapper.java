package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

public class TopKTaxisMapper extends Mapper<Text, Text, Text, FloatWritable> {
  private PriorityQueue<TaxiPriority> priorityQueue;
  private int k;

  @Override
  public void setup(Context context) {
    priorityQueue = new PriorityQueue<>();
    k = context.getConfiguration().getInt("k", 5);
  }

  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    priorityQueue.add(
        new TaxiPriority(
            new Text(key),
            new FloatWritable(Float.parseFloat(value.toString()))));
    if (priorityQueue.size() > k) {
      priorityQueue.poll();
    }
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    while (!priorityQueue.isEmpty()) {
      TaxiPriority taxiPriority = priorityQueue.poll();
      context.write(taxiPriority.getTaxiId(), taxiPriority.getPriority());
    }
  }
}
