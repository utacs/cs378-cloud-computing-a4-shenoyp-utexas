package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class TaxiToAvgEarningsMapper extends Mapper<Object, Text, Text, DualFloatWritable> {
  private static final int expectedTokensCount = 17;

  private static final int taxiIdIndex = 0;

  private static final int tripTimeIndex = 4;

  private static final int totalAmountIndex = 16;

  private static final double maxTotalAmount = 500;

  private static final Pattern positiveIntPattern = Pattern.compile("[1-9]+[0-9]*");

  private static final Pattern floatPattern = Pattern.compile("[+-]?([0-9]*[.])?[0-9]+");

  private final Text taxi = new Text();

  private final DualFloatWritable earningsMinutes = new DualFloatWritable(0, 0);

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] tokens = value.toString().split(",");

    try {
      if (tokens.length != expectedTokensCount ||
          tokens[taxiIdIndex].isEmpty() ||
          tokens[1].isEmpty() ||
          !positiveIntPattern.matcher(tokens[tripTimeIndex]).matches() ||
          !floatPattern.matcher(tokens[totalAmountIndex]).matches() ||
          Integer.parseInt(tokens[tripTimeIndex]) <= TimeUnit.MINUTES.toSeconds(1) ||
          Float.parseFloat(tokens[totalAmountIndex]) > maxTotalAmount) {
        return;
      }
    } catch (NumberFormatException e) {
      return;
    }

    taxi.set(tokens[taxiIdIndex]);
    earningsMinutes.setFirst(Float.parseFloat(tokens[totalAmountIndex]));
    earningsMinutes.setSecond(((float) Integer.parseInt(tokens[tripTimeIndex])) / 60);
    context.write(taxi, earningsMinutes);
  }
}
