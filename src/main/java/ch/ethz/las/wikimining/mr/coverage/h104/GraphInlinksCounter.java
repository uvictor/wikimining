
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.base.IntArrayWritable;
import ch.ethz.las.wikimining.mr.io.h104.SetupHelper;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Given the inverted adjacency lists it computes the number of inlinks.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GraphInlinksCounter extends Configured implements Tool {
  private static final Logger logger = Logger.getLogger(GreeDiFirst.class);

  private static enum Records {

    TOTAL
  };

  private static class Map extends MapReduceBase implements Mapper<
    IntWritable, IntArrayWritable, IntWritable, IntWritable> {

    @Override
    public void map(IntWritable key, IntArrayWritable value,
        OutputCollector<IntWritable, IntWritable> output,
        Reporter reporter) throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);

      output.collect(key, new IntWritable(value.get().length));
    }
  }

  private String inputPath;
  private String outputPath;

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    JobConf config = new JobConf(getConf(), GreeDiFirst.class);
    config.setJobName("Graph-InlinksCounter");

    config.setNumReduceTasks(0);

    SetupHelper.getInstance()
        .setSequenceInput(config, inputPath)
        .setSequenceOutput(config, outputPath);

    config.setOutputKeyClass(IntWritable.class);
    config.setOutputValueClass(IntWritable.class);

    config.setMapperClass(Map.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    JobClient.runJob(config);

    return 0;
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Adjacency lists").create(Fields.INPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Inlinks count").create(Fields.OUTPUT.get()));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(Fields.INPUT.get())
        || !cmdline.hasOption(Fields.OUTPUT.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - output: " + outputPath);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GraphInlinksCounter(), args);
  }
}
