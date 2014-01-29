package ch.ethz.las.wikimining.mr;

import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
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
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

/**
 * Computes the word coverage as equation (1) from the paper, from a Mahout
 * index, using MapReduce and the GreeDi protocol. First pass.
 * @deprecated for Hadoop 1.0.4
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WordCoverageFirstGreeDiH104 extends Configured implements Tool {

  private static final int DEFAULT_PARTITION_COUNT = 400;
  private static final String PARTITION_COUNT_FIELD = "PartitionCountField";
  private static final int DEFAULT_SELECT_COUNT = 10;
  private static final String SELECT_COUNT_FIELD = "SelectCountField";

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String PARTITION_COUNT_OPTION = "partitions";
  private static final String SELECT_COUNT_OPTION = "select";

  private static final Logger logger =
      Logger.getLogger(WordCoverageFirstGreeDiH104.class);

  private static class Map extends MapReduceBase implements Mapper<
      Text, VectorWritable, IntWritable, DocumentWithVectorWritable> {

    int partitionCount;

    @Override
    public void configure(JobConf job) {
      partitionCount =
          job.getInt(PARTITION_COUNT_FIELD, DEFAULT_PARTITION_COUNT);
    }

    @Override
    public void map(Text key, VectorWritable value, OutputCollector<IntWritable,
        DocumentWithVectorWritable> output, Reporter reporter)
        throws IOException {
      final int partition = Integer.parseInt(key.toString()) % partitionCount;
      final IntWritable outKey = new IntWritable(partition);

      final DocumentWithVectorWritable outValue =
          new DocumentWithVectorWritable(key, value);

      output.collect(outKey, outValue);
    }
  }

  private static class Reduce extends MapReduceBase implements Reducer<
      IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

    int selectCount;

    @Override
    public void configure(JobConf job) {
      selectCount = job.getInt(SELECT_COUNT_FIELD, DEFAULT_SELECT_COUNT);
    }

    @Override
    public void reduce(IntWritable key,
        Iterator<DocumentWithVectorWritable> values,
        OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
        throws IOException {
      final WordCoverageFromMahout objectiveFunction =
          new WordCoverageFromMahout(values);
      logger.info("Created WordCoverageFromMahout");
      final SfoGreedyAlgorithm sfo =
          new SfoGreedyLazy(objectiveFunction, reporter);
      logger.info("Created SfoGreedyAlgorithm");
      Set<Integer> selected =
          sfo.run(objectiveFunction.getAllDocIds(), selectCount);
      logger.info("Finished running SFO");

      for (Integer docId : selected) {
        IntWritable outValue = new IntWritable(docId);
        output.collect(NullWritable.get(), outValue);
      }
    }
  }

  private String inputPath;
  private String outputPath;
  private int partitionCount;
  private int selectCount;

  public WordCoverageFirstGreeDiH104() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    JobConf conf = new JobConf(getConf(), WordCoverageFirstGreeDiH104.class);
    conf.setJobName(String.format(
        "DocumentWordCoverage[%s %s]", partitionCount, selectCount));

    conf.setInt(PARTITION_COUNT_FIELD, partitionCount);
    conf.setInt(SELECT_COUNT_FIELD, selectCount);

    conf.setNumReduceTasks(partitionCount);

    SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    FileOutputFormat.setCompressOutput(conf, false);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(DocumentWithVectorWritable.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    JobClient.runJob(conf);

    return 0;
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Tfidf vectors").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Selected articles").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("integer").hasArg()
        .withDescription("Partition count").create(PARTITION_COUNT_OPTION));
    options.addOption(OptionBuilder.withArgName("integer").hasArg()
        .withDescription("Select count").create(SELECT_COUNT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(INPUT_OPTION);
    outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

    partitionCount = DEFAULT_PARTITION_COUNT;
    if (cmdline.hasOption(PARTITION_COUNT_OPTION)) {
      partitionCount =
          Integer.parseInt(cmdline.getOptionValue(PARTITION_COUNT_OPTION));
      if(partitionCount <= 0){
        System.err.println(
            "Error: \"" + partitionCount + "\" has to be positive!");
        return -1;
      }
    }
    selectCount = Integer.parseInt(cmdline.getOptionValue(SELECT_COUNT_OPTION));

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - partitions: " + partitionCount);
    logger.info(" - select: " + selectCount);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new WordCoverageFirstGreeDiH104(), args);
  }
}
