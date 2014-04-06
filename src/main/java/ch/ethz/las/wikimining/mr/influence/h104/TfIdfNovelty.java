package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.base.HashBandWritable;
import ch.ethz.las.wikimining.base.IntArrayWritable;
import ch.ethz.las.wikimining.mr.utils.h104.SetupHelper;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

/**
 * Computes the novelty tf-idf according to equation 3.
 * Finds k-nearest documents from the past, for each document.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfNovelty extends Configured implements Tool {

  static enum Records {

    TOTAL,
    POTENTIALLY_IGNORED
  };

  private static final Logger logger = Logger.getLogger(TfIdfNovelty.class);

  private String inputPath;
  private String outputPath;
  private String basisPath;
  private String datesPath;
  private boolean ignoreDocs;
  private boolean outputBuckets;
  private int bands;
  private int rows;

  public TfIdfNovelty() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    JobConf config = new JobConf(getConf(), TfIdfNovelty.class);
    config.setJobName("Influence-TfIdfNovelty");

    config.set(Fields.BASIS.get(), basisPath);
    if (datesPath != null) {
      config.set(Fields.DOC_DATES.get(), datesPath);
    }
    config.setBoolean(Fields.IGNORE.get(), ignoreDocs);
    if (bands > 0) {
      config.setInt(Fields.BANDS.get(), bands);
    }
    if (rows > 0) {
      config.setInt(Fields.ROWS.get(), rows);
    }

    SetupHelper.getInstance()
        .setSequenceInput(config, inputPath)
        .setSequenceOutput(config, outputPath);

    config.setMapOutputKeyClass(HashBandWritable.class);
    config.setMapOutputValueClass(DocumentWithVectorWritable.class);
    config.setMapperClass(TfIdfNoveltyLshMapper.class);

    if (outputBuckets) {
      config.setOutputKeyClass(HashBandWritable.class);
      config.setOutputValueClass(IntArrayWritable.class);
      config.setReducerClass(TfIdfNoveltyIdentityReducer.class);
    } else {
      config.setOutputKeyClass(Text.class);
      config.setOutputValueClass(VectorWritable.class);
      config.setReducerClass(TfIdfNoveltyReducer.class);
    }

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
        .withDescription("Vectors' length").create(Fields.BASIS.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Near documents").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Document dates").create(Fields.DOC_DATES.get()));
    options.addOption(OptionBuilder
        .withDescription("Ignore docs without NN").create(Fields.IGNORE.get()));
    options.addOption(OptionBuilder
        .withDescription("Output buckets").create(Fields.OUTPUT_BUCKETS.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Number of bands").create(Fields.BANDS.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Number of rows").create(Fields.ROWS.get()));

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
        || !cmdline.hasOption(Fields.BASIS.get())
        || (!cmdline.hasOption(Fields.DOC_DATES.get())
        && !cmdline.hasOption(Fields.OUTPUT_BUCKETS.get()))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());
    basisPath = cmdline.getOptionValue(Fields.BASIS.get());
    datesPath = cmdline.getOptionValue(Fields.DOC_DATES.get());

    ignoreDocs = false;
    if (cmdline.hasOption(Fields.IGNORE.get())) {
      ignoreDocs = true;
    }
    outputBuckets = false;
    if (cmdline.hasOption(Fields.OUTPUT_BUCKETS.get())) {
      outputBuckets = true;
    }
    bands = -1;
    if (cmdline.hasOption(Fields.BANDS.get())) {
      bands = Integer.parseInt(cmdline.getOptionValue(Fields.BANDS.get()));
    }
    rows = -1;
    if (cmdline.hasOption(Fields.ROWS.get())) {
      rows = Integer.parseInt(cmdline.getOptionValue(Fields.ROWS.get()));
    }

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - basis: " + basisPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - dates: " + datesPath);
    logger.info(" - ignore: " + ignoreDocs);
    logger.info(" - outputBuckets: " + outputBuckets);
    logger.info(" - bands: " + bands);
    logger.info(" - rows: " + rows);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TfIdfNovelty(), args);
  }
}
