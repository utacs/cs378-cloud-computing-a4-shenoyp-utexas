package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

public class HourToGpsErrorCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
  private static final DateTimeFormatter datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final int pickupDatetimeIndex = 3;
  private static final int[] gpsCoordinateIndices = new int[] {6, 7, 8, 9};
  static int x = 0;

  private final IntWritable hour = new IntWritable(0);
  private final IntWritable counter = new IntWritable(1);

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] tokens = value.toString().split(",");

    final int expectedTokensCount = 17;
    if (tokens.length != expectedTokensCount) {
      return;
    }

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

    if (!hasError) {
      return;
    }

    try {
      int hourOfDay = datetimeFormatter
          .parse(tokens[pickupDatetimeIndex])
          .get(ChronoField.HOUR_OF_DAY);
      hour.set(hourOfDay + 1);
    } catch (DateTimeException e) {
      return;
    }

    context.write(hour, counter);
  }
}
