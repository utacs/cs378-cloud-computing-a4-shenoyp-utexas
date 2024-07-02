package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DualFloatWritable implements Writable {
  private float first;
  private float second;

  @SuppressWarnings("unused")
  public DualFloatWritable() {}

  public DualFloatWritable(float first, float second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeFloat(first);
    dataOutput.writeFloat(second);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    first = dataInput.readFloat();
    second = dataInput.readFloat();
  }

  public float getFirst() {
    return first;
  }

  public float getSecond() {
    return second;
  }

  public void setFirst(float first) {
    this.first = first;
  }

  public void setSecond(float second) {
    this.second = second;
  }
}
