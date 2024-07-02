package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

public class TopKTaxisReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
  private PriorityQueue<TaxiPriority> priorityQueue;
  private int k;

  @Override
  protected void setup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
    priorityQueue = new PriorityQueue<>();
    k = context.getConfiguration().getInt("k", 5);
  }

  @Override
  protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
    for (FloatWritable value : values) {
      priorityQueue.add(new TaxiPriority(new Text(key), new FloatWritable(value.get())));
    }

    while (priorityQueue.size() > k) {
      priorityQueue.poll();
    }
  }

  @Override
  protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
    List<TaxiPriority> values = new ArrayList<>(k);
    while (!priorityQueue.isEmpty()) {
      values.add(priorityQueue.poll());
    }

    Collections.reverse(values);
    for (TaxiPriority value : values) {
      context.write(value.getTaxiId(), value.getPriority());
    }
  }
}
