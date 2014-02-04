package ch.ethz.las.wikimining.mr.influence;

import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.utils.IntegerSequenceFileReader;
import ch.ethz.las.wikimining.mr.utils.SetupHelper;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

/**
 * Computes tf-idf word spread according to equation 4.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfWordSpread extends Configured implements Tool {

  private static enum Records {

    TOTAL,
    DATES
  };

  private static class Map extends Mapper<
      Text, VectorWritable, IntWritable, VectorWritable> {

    private HashMap<Integer, Integer> docDates;

    @Override
    public void setup(Context context) {
      try {
        Path datesPath =
            new Path(context.getConfiguration().get(Fields.DOC_DATES.get()));
        logger.info("Loading doc dates: " + datesPath);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        final IntegerSequenceFileReader datesReader =
            new IntegerSequenceFileReader(
                datesPath, fs, context.getConfiguration());
        docDates = datesReader.read();
      } catch (IOException e) {
        logger.fatal("Error loading doc dates!", e);
      }
      logger.info("Loaded " + docDates.size() + " doc dates.");
    }

    @Override
    public void map(Text docId, VectorWritable value, Mapper.Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      final int date = docDates.get(Integer.parseInt(docId.toString()));
      context.write(new IntWritable(date), value);
    }
  }

  private static class Reduce extends Reducer<
      IntWritable, VectorWritable, IntWritable, VectorWritable> {

    @Override
    public void reduce(IntWritable date,
        Iterable<VectorWritable> docs, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.DATES).increment(1);
      final Iterator<VectorWritable> it = docs.iterator();
      final RandomAccessSparseVector sum =
          new RandomAccessSparseVector(it.next().get());

      while(it.hasNext()) {
        sum.plus(it.next().get());
      }

      context.write(date, new VectorWritable(sum));
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

    Job job = Job.getInstance(getConf());
    job.setJarByClass(TfIdfWordSpread.class);
    job.setJobName("Influence-TfidfWordSpread");

    job.getConfiguration().set(Fields.DOC_DATES.get(), datesPath);

    SetupHelper.getInstance()
        .setSequenceInput(job, inputPath)
        .setSequenceOutput(job, outputPath);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

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
