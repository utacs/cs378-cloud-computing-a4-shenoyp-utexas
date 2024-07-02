package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaxiToGpsErrorRateMapper extends Mapper<Object, Text, Text, FloatWritable> {
  private static final int expectedTokensCount = 17;

  private static final int taxiIdIndex = 0;

  private static final int[] gpsCoordinateIndices = new int[] {6, 7, 8, 9};

  private final Text taxi = new Text();

  private final FloatWritable zero = new FloatWritable(0);

  private final FloatWritable one = new FloatWritable(1);

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] tokens = value.toString().split(",");

    if (tokens.length != expectedTokensCount) {
      return;
    }

    if (tokens[taxiIdIndex].isEmpty()) {
      return;
    }
    taxi.set(tokens[taxiIdIndex]);

    boolean hasError = false;
    for (int index : gpsCoordinateIndices) {
      if (tokens[index].isEmpty()) {
        return;
      }

      try {
        if (Float.parseFloat(tokens[index]) == 0) {
          hasError = true;
        }
      } catch (NumberFormatException e) {
        return;
      }
    }

    context.write(taxi, hasError ? one : zero);
  }
}
