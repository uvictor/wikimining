
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.base.IntArrayWritable;
import ch.ethz.las.wikimining.mr.utils.h104.SetupHelper;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Converts a list of revisions' stats to sequence files.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class RevisionsConvertor extends Configured implements Tool {

  private static enum Records {

    TOTAL
  };

  private static class Map extends MapReduceBase implements Mapper<
      LongWritable, Text, IntWritable, IntArrayWritable> {

    boolean selectSecondStat;

    @Override
    public void configure(JobConf config) {
      selectSecondStat = config.getBoolean(Fields.SECOND_STAT.get(), false);
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, IntArrayWritable> output,
        Reporter reporter) throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);

      final Scanner scanner = new Scanner(value.toString());
      final IntWritable outKey = new IntWritable(scanner.nextInt());

      final ArrayList<IntWritable> revisions = new ArrayList<>(2);
      revisions.add(new IntWritable(scanner.nextInt()));
      if (selectSecondStat) {
        revisions.add(new IntWritable(scanner.nextInt()));
      }
      final IntArrayWritable outValue =
          new IntArrayWritable(revisions.toArray(new IntWritable[0]));

      output.collect(outKey, outValue);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    JobConf config = new JobConf(getConf(), GreeDiFirst.class);
    config.setJobName("Graph - EdgeConvertor");

    SetupHelper.getInstance()
        .setTextInput(config, args[0])
        .setSequenceOutput(config, args[1]);

    config.setOutputKeyClass(IntWritable.class);
    config.setOutputValueClass(IntArrayWritable.class);

    config.setMapperClass(Map.class);

    if (args.length >= 3) {
      config.setBoolean(Fields.SECOND_STAT.get(), true);
    }

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(args[1]), true);

    JobClient.runJob(config);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RevisionsConvertor(), args);
  }
}
