package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class TaxiPriority implements Comparable<TaxiPriority> {
  private final Text taxiId;
  private final FloatWritable priority;

  public TaxiPriority(Text taxiId, FloatWritable priority) {
    this.taxiId = taxiId;
    this.priority = priority;
  }

  public Text getTaxiId() {
    return taxiId;
  }

  public FloatWritable getPriority() {
    return priority;
  }

  @Override
  public int compareTo(TaxiPriority other) {
    return Float.compare(priority.get(), other.getPriority().get());
  }
}
