package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.utils.h104.SetupHelper;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

/**
 * Computes the word coverage as equation (1) from the paper, from a Mahout
 * index, using MapReduce and the GreeDi protocol. First pass.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GreeDiFirst extends Configured implements Tool {

  private static final Logger logger = Logger.getLogger(GreeDiFirst.class);

  private static enum Records {

    TOTAL
  };

  private static class Map extends MapReduceBase implements Mapper<
      Text, VectorWritable, IntWritable, DocumentWithVectorWritable> {

    int partitionCount;

    @Override
    public void configure(JobConf job) {
      partitionCount = job.getInt(
          Fields.PARTITION_COUNT.get(), Defaults.PARTITION_COUNT.get());
    }

    @Override
    public void map(Text key, VectorWritable value,
        OutputCollector<IntWritable, DocumentWithVectorWritable> output,
        Reporter reporter) throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);
      final int partition = Integer.parseInt(key.toString()) % partitionCount;
      final IntWritable outKey = new IntWritable(partition);

      final DocumentWithVectorWritable outValue =
          new DocumentWithVectorWritable(key, value);

      output.collect(outKey, outValue);
    }
  }

  private String inputPath;
  private String outputPath;
  private String wordCountPath;
  private String wordCountType;
  private String bucketsPath;
  private String graphPath;
  private String revisionsPath;
  private int partitionCount;
  private int selectCount;

  public GreeDiFirst() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    JobConf config = new JobConf(getConf(), GreeDiFirst.class);
    config.setJobName(String.format(
        "Coverage-GreeDiFirst[%s %s]", partitionCount, selectCount));

    config.setInt(Fields.PARTITION_COUNT.get(), partitionCount);
    config.setInt(Fields.SELECT_COUNT.get(), selectCount);

    config.setNumReduceTasks(partitionCount);

    SetupHelper.getInstance()
        .setSequenceInput(config, inputPath)
        .setTextOutput(config, outputPath);

    config.setMapOutputKeyClass(IntWritable.class);
    config.setMapOutputValueClass(DocumentWithVectorWritable.class);
    config.setOutputKeyClass(NullWritable.class);
    config.setOutputValueClass(IntWritable.class);

    config.setMapperClass(Map.class);
    if (wordCountPath != null) {
      config.set(Fields.WORD_COUNT.get(), wordCountPath);
      config.set(Fields.WORD_COUNT_TYPE.get(), wordCountType);
    }
    if (bucketsPath != null) {
      config.set(Fields.BUCKETS.get(), bucketsPath);
      //config.setReducerClass(LshBucketsGreeDiReducer.class);
    }
    if (graphPath != null) {
      config.set(Fields.GRAPH.get(), graphPath);
      //config.setReducerClass(CombinerGreeDiReducer.class);
    }
    if (revisionsPath != null) {
      config.set(Fields.REVISIONS.get(), revisionsPath);
    }
    config.setReducerClass(CombinerGreeDiReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    JobClient.runJob(config);

    return 0;
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Tfidf vectors").create(Fields.INPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Selected articles").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Word counts").create(Fields.WORD_COUNT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Word counts type")
        .create(Fields.WORD_COUNT_TYPE.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Buckets").create(Fields.BUCKETS.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Graph").create(Fields.GRAPH.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Revisions").create(Fields.REVISIONS.get()));
    options.addOption(OptionBuilder.withArgName("integer").hasArg()
        .withDescription("Partition count")
        .create(Fields.PARTITION_COUNT.get()));
    options.addOption(OptionBuilder.withArgName("integer").hasArg()
        .withDescription("Select count").create(Fields.SELECT_COUNT.get()));

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
    wordCountPath = cmdline.getOptionValue(Fields.WORD_COUNT.get());
    wordCountType = cmdline.getOptionValue(Fields.WORD_COUNT_TYPE.get());
    bucketsPath = cmdline.getOptionValue(Fields.BUCKETS.get());
    graphPath = cmdline.getOptionValue(Fields.GRAPH.get());
    revisionsPath = cmdline.getOptionValue(Fields.REVISIONS.get());

    partitionCount = Defaults.PARTITION_COUNT.get();
    if (cmdline.hasOption(Fields.PARTITION_COUNT.get())) {
      partitionCount =
          Integer.parseInt(cmdline.getOptionValue(Fields.PARTITION_COUNT.get()));
      if(partitionCount <= 0){
        System.err.println(
            "Error: \"" + partitionCount + "\" has to be positive!");
        return -1;
      }
    }
    selectCount =
        Integer.parseInt(cmdline.getOptionValue(Fields.SELECT_COUNT.get()));

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - wordCount: " + wordCountPath);
    logger.info(" - wordCountType: " + wordCountType);
    logger.info(" - buckets: " + bucketsPath);
    logger.info(" - graph: " + graphPath);
    logger.info(" - graph: " + revisionsPath);
    logger.info(" - partitions: " + partitionCount);
    logger.info(" - select: " + selectCount);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GreeDiFirst(), args);
  }
}
