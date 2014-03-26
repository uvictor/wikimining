
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.mr.base.IntArrayWritable;
import ch.ethz.las.wikimining.mr.utils.h104.SetupHelper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Converts a list of edges to a edge list for each node.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GraphEdgesConvertor extends Configured implements Tool {

  private static enum Records {

    TOTAL
  };

  private static class Map extends MapReduceBase implements Mapper<
      LongWritable, Text, IntWritable, IntWritable> {

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, IntWritable> output,
        Reporter reporter) throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);

      final Scanner scanner = new Scanner(value.toString());

      output.collect(new IntWritable(scanner.nextInt()),
          new IntWritable(scanner.nextInt()));
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<
    IntWritable, IntWritable, IntWritable, IntArrayWritable> {

    @Override
    public void reduce(IntWritable key, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntArrayWritable> output,
        Reporter reporter) throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);

      // Remove duplicates (unfortunately we do have them).
      final HashSet<IntWritable> edges = new HashSet<>();
      while(values.hasNext()) {
        edges.add(new IntWritable(values.next().get()));
      }

    output
        .collect(key, new IntArrayWritable(edges.toArray(new IntWritable[0])));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    JobConf config = new JobConf(getConf(), GreeDiFirst.class);
    config.setJobName("Graph - EdgeConvertor");

    SetupHelper.getInstance()
        .setTextInput(config, args[0])
        .setSequenceOutput(config, args[1]);

    config.setMapOutputKeyClass(IntWritable.class);
    config.setMapOutputValueClass(IntWritable.class);
    config.setOutputKeyClass(IntWritable.class);
    config.setOutputValueClass(IntArrayWritable.class);

    config.setMapperClass(Map.class);
    config.setReducerClass(Reduce.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(args[1]), true);

    JobClient.runJob(config);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GraphEdgesConvertor(), args);
  }
}
