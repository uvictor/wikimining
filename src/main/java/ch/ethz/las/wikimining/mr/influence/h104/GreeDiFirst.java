package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
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
 * Computes the influence of a document as equation (5) from the paper,
 * from a novelty document index and a yearly word spread index, using MapReduce
 * and the GreeDi protocol. First pass.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GreeDiFirst extends Configured implements Tool {

  private static final Logger logger = Logger.getLogger(GreeDiFirst.class);

  private static class Map extends MapReduceBase implements Mapper<
      Text, VectorWritable, IntWritable, DocumentWithVectorWritable> {

    private int partitionCount;

    @Override
    public void configure(JobConf config) {
      partitionCount = config.getInt(
          Fields.PARTITION_COUNT.get(), Defaults.PARTITION_COUNT.get());
    }

    @Override
    public void map(Text key, VectorWritable value,
        OutputCollector<IntWritable, DocumentWithVectorWritable> output,
        Reporter reporter) throws IOException {

      final int partition = Integer.parseInt(key.toString()) % partitionCount;
      final IntWritable outKey = new IntWritable(partition);

      final DocumentWithVectorWritable outValue =
          new DocumentWithVectorWritable(key, value);

      output.collect(outKey, outValue);
    }
  }

  private String inputPath;
  private String outputPath;
  private String datesPath;
  private String wordSpreadPath;
  private String info;
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
        "Influence-GreeDiFirst[%s %s %s]", info, partitionCount, selectCount));

    config.set(Fields.DOC_DATES.get(), datesPath);
    config.set(Fields.WORD_SPREAD.get(), wordSpreadPath);
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
    config.setReducerClass(GreeDiReducer.class);

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
        .withDescription("Document dates").create(Fields.DOC_DATES.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Word spread yearly matrix")
        .create(Fields.WORD_SPREAD.get()));

    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("Job info").create(Fields.INFO.get()));
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

    if (!cmdline.hasOption(Fields.INPUT.get())
        || !cmdline.hasOption(Fields.OUTPUT.get())
        || !cmdline.hasOption(Fields.DOC_DATES.get())
        || !cmdline.hasOption(Fields.WORD_SPREAD.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());
    datesPath = cmdline.getOptionValue(Fields.DOC_DATES.get());
    wordSpreadPath = cmdline.getOptionValue(Fields.WORD_SPREAD.get());
    info = cmdline.getOptionValue(Fields.INFO.get());

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
    logger.info(" - dates: " + datesPath);
    logger.info(" - spread: " + wordSpreadPath);
    logger.info(" - partitions: " + partitionCount);
    logger.info(" - select: " + selectCount);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GreeDiFirst(), args);
  }
}
