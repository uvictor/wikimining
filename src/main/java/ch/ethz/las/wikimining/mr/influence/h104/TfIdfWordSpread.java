package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.mr.utils.h104.IntegerSequenceFileReader;
import ch.ethz.las.wikimining.mr.utils.h104.SetupHelper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

/**
 * Computes tf-idf word spprocessContent according to equation 4.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfWordSpread extends Configured implements Tool {

  private static enum Records {

    TOTAL,
    DATES
  };

  private static class Map extends MapReduceBase implements Mapper<
      Text, VectorWritable, IntWritable, VectorWritable> {

    private HashMap<Integer, Integer> docDates;

    @Override
    public void configure(JobConf config) {
      try {
        Path datesPath =
            new Path(config.get(Fields.DOC_DATES.get()));
        logger.info("Loading doc dates: " + datesPath);

        FileSystem fs = FileSystem.get(config);
        final IntegerSequenceFileReader datesReader =
            new IntegerSequenceFileReader(
                datesPath, fs, config);
        docDates = datesReader.processFile();
      } catch (IOException e) {
        logger.fatal("Error loading doc dates!", e);
      }
      logger.info("Loaded " + docDates.size() + " doc dates.");
    }

    @Override
    public void map(Text docId, VectorWritable value,
        OutputCollector<IntWritable, VectorWritable> output, Reporter reporter)
        throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);

      final int id = Integer.parseInt(docId.toString());
      final int date = docDates.get(id);
      output.collect(new IntWritable(date), value);
    }
  }

  private static class Reduce extends MapReduceBase implements Reducer<
      IntWritable, VectorWritable, IntWritable, VectorWritable> {

    @Override
    public void reduce(IntWritable date, Iterator<VectorWritable> docs,
        OutputCollector<IntWritable, VectorWritable> output, Reporter reporter)
        throws IOException {
      reporter.getCounter(Records.DATES).increment(1);
      final RandomAccessSparseVector sum =
          new RandomAccessSparseVector(docs.next().get());

      // TODO(uvictor): consider 2-normalizing these vectors?
      while(docs.hasNext()) {
        sum.plus(docs.next().get());
      }

      output.collect(date, new VectorWritable(sum));
    }
  }

  private static final Logger logger =
      Logger.getLogger(TfIdfWordSpread.class);

  private String inputPath;
  private String outputPath;
  private String datesPath;

  public TfIdfWordSpread() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    JobConf config = new JobConf(getConf(), TfIdfWordSpread.class);
    config.setJobName("Influence-TfidfWordSpread");

    config.set(Fields.DOC_DATES.get(), datesPath);

    SetupHelper.getInstance()
        .setSequenceInput(config, inputPath)
        .setSequenceOutput(config, outputPath);

    config.setMapOutputKeyClass(IntWritable.class);
    config.setMapOutputValueClass(VectorWritable.class);
    config.setOutputKeyClass(IntWritable.class);
    config.setOutputValueClass(VectorWritable.class);

    config.setMapperClass(Map.class);
    config.setReducerClass(Reduce.class);

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
        .withDescription("Near documents").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Document dates").create(Fields.DOC_DATES.get()));

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
        || !cmdline.hasOption(Fields.DOC_DATES.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());
    datesPath = cmdline.getOptionValue(Fields.DOC_DATES.get());

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - dates: " + datesPath);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TfIdfWordSpread(), args);
  }
}
