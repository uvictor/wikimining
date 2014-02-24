package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.base.HashBandWritable;
import ch.ethz.las.wikimining.mr.utils.h104.MatrixSequenceFileReader;
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
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.VectorFunction;

/**
 * Computes the novelty tf-idf according to equation 3.
 * Finds k-nearest documents from the past, for each document.
 * Uses Locality Sensitive Hashing with Random Projections (for cosine
 * similarity).
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfNovelty extends Configured implements Tool {

  static enum Records {

    TOTAL,
    POTENTIALLY_IGNORED
  };

  private static class Map extends MapReduceBase implements Mapper<
      Text, VectorWritable, HashBandWritable, DocumentWithVectorWritable> {

    private Matrix basisMatrix;
    private int bandCount;
    private int rowCount;

    @Override
    public void configure(JobConf config) {
      try {
        final Path basisPath =
            new Path(config.get(Fields.BASIS.get()));
        FileSystem fs = FileSystem.get(config);

        final MatrixSequenceFileReader basisReader =
            new MatrixSequenceFileReader(basisPath.suffix("/part-m-00000"),
                fs, config);
        basisMatrix = basisReader.read();
      } catch (IOException e) {
        logger.fatal("Error loading basis matrix!", e);
      }

      bandCount = config.getInt(Fields.BANDS.get(), Defaults.BANDS.get());
      rowCount = config.getInt(Fields.ROWS.get(), Defaults.ROWS.get());
    }

    @Override
    public void map(Text docId, VectorWritable value,
        OutputCollector<HashBandWritable, DocumentWithVectorWritable> output,
        Reporter reporter) throws IOException {
      reporter.getCounter(Records.TOTAL).increment(1);
      final Vector vector = value.get();

      final Vector rowHashes =
          basisMatrix.aggregateRows(new VectorFunction() {
            @Override
            /**
             * Signum of the dot product with a random projection.
             * Cosine similarity.
             */
            public double apply(Vector projector) {
              return projector.dot(vector) > 0 ? 1231 : 1237;
            }
          });

      // TODO(uvictor): consider using multiple hash functions?
      for (int band = 0; band < bandCount; band++) {
        final Vector bandVector = rowHashes.viewPart(band * rowCount, rowCount);
        final int hash = new VectorWritable(bandVector).hashCode();
        output.collect(new HashBandWritable(hash, band),
            new DocumentWithVectorWritable(docId, value));
      }
    }
  }

  private static final Logger logger = Logger.getLogger(TfIdfNovelty.class);

  private String inputPath;
  private String outputPath;
  private String basisPath;
  private String datesPath;
  private boolean ignoreDocs;
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
    config.set(Fields.DOC_DATES.get(), datesPath);
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
    config.setOutputKeyClass(Text.class);
    config.setOutputValueClass(VectorWritable.class);

    config.setMapperClass(Map.class);
    config.setReducerClass(TfIdfNoveltyReducer.class);

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
        || !cmdline.hasOption(Fields.DOC_DATES.get())) {
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
    logger.info(" - bands: " + bands);
    logger.info(" - rows: " + rows);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TfIdfNovelty(), args);
  }
}
