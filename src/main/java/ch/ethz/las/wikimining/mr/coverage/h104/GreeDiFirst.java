package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
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
public class GreeDiFirst extends Configured implements Tool {

  private static final Logger logger =
      Logger.getLogger(GreeDiFirst.class);

  private static enum Records {

    TOTAL
  };

  private static class Map extends MapReduceBase implements Mapper<
      Text, VectorWritable, IntWritable, DocumentWithVectorWritable> {

    int partitionCount;

    @Override
    public void configure(JobConf job) {
      partitionCount =
          job.getInt(Fields.PARTITION_COUNT.get(), Defaults.PARTITION_COUNT.get());
    }

    @Override
    public void map(Text key, VectorWritable value, OutputCollector<IntWritable,
        DocumentWithVectorWritable> output, Reporter reporter)
        throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);
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
      selectCount = job.getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());
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

  public GreeDiFirst() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    JobConf conf = new JobConf(getConf(), GreeDiFirst.class);
    conf.setJobName(String.format(
        "Coverage-GreeDiFirst[%s %s]", partitionCount, selectCount));

    conf.setInt(Fields.PARTITION_COUNT.get(), partitionCount);
    conf.setInt(Fields.SELECT_COUNT.get(), selectCount);

    conf.setNumReduceTasks(partitionCount);

    SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

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
        .withDescription("Tfidf vectors").create(Fields.INPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Selected articles").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("integer").hasArg()
        .withDescription("Partition count").create(Fields.PARTITION_COUNT.get()));
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

    if (!cmdline.hasOption(Fields.INPUT.get()) || !cmdline.hasOption(Fields.OUTPUT.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());

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
    selectCount = Integer.parseInt(cmdline.getOptionValue(Fields.SELECT_COUNT.get()));

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - partitions: " + partitionCount);
    logger.info(" - select: " + selectCount);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GreeDiFirst(), args);
  }
}
