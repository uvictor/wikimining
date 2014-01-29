
package ch.ethz.las.wikimining.mr;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.VectorFunction;
import org.apache.mahout.math.random.RandomProjector;

/**
 * Find k-nearest documents from the past, for each document.
 * Uses Locality Sensitive Hashing with Random Projections (for cosine
 * similarity).
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class NovelNearestDocs extends Configured implements Tool {

  private static enum Records {

    TOTAL
  };

  /**
   * Object to set the Text type to ArrayWritable, so that we can use
   * "hadoop fs -text" for reading the arrays.
   */
  public static class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
      super(Text.class);
    }

    public TextArrayWritable(Text[] values) {
      super(Text.class, values);
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      for (String element : toStrings()) {
        stringBuilder.append(element).append(' ');
      }

      return stringBuilder.toString();
    }
  }

  private static class Map extends
      Mapper<Text, VectorWritable, HashBandWritable, Text> {

    private Matrix basisMatrix;

    @Override
    public void setup(Context context) {
      final int bandsCount = context.getConfiguration()
          .getInt(BANDS_FIELD, DEFAULT_BANDS);
      final int rowsCount = context.getConfiguration()
          .getInt(ROWS_FIELD, DEFAULT_ROWS);
      final int dimensions = context.getConfiguration()
          .getInt(DIMENSIONS_FIELD, DEFAULT_DIMENSIONS);

      // TODO(uvictor): use the same basisMatrix for all tasks !!
      basisMatrix = RandomProjector
          .generateBasisPlusMinusOne(bandsCount * rowsCount, dimensions);
    }

    @Override
    public void map(Text docId, VectorWritable value, Mapper.Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      final int bandCount = context.getConfiguration()
          .getInt(BANDS_FIELD, DEFAULT_BANDS);
      final int rowCount = context.getConfiguration()
          .getInt(ROWS_FIELD, DEFAULT_ROWS);
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
        context.write(new HashBandWritable(hash, band), docId);
      }
    }
  }

  private static class Reduce extends Reducer<
      HashBandWritable, Text, HashBandWritable, TextArrayWritable> {

    @Override
    protected void reduce(
        HashBandWritable key, Iterable<Text> docIds, Context context)
        throws IOException, InterruptedException {
      final ArrayList<Text> nearDocs = new ArrayList<>();
      for (Text docId : docIds) {
        nearDocs.add(new Text(docId));
      }

      context.write(key, new TextArrayWritable(nearDocs.toArray(new Text[0])));
    }
  }

  private static final int DEFAULT_DIMENSIONS = -1;
  private static final String DIMENSIONS_FIELD = "DimensionsField";
  private static final int DEFAULT_BANDS = 9;
  private static final String BANDS_FIELD = "BandsField";
  private static final int DEFAULT_ROWS = 13;
  private static final String ROWS_FIELD = "RowsField";

  private static final String INPUT_OPTION = "input";
  private static final String DIMENSIONS_OPTION = "dimensions";
  private static final String OUTPUT_OPTION = "output";
  private static final String BANDS_OPTION = "bands";
  private static final String ROWS_OPTION = "rows";

  private static final Logger logger =
      Logger.getLogger(WordCoverageFirstGreeDi.class);

  private String inputPath;
  private String outputPath;
  private int dimensions;
  private int bands;
  private int rows;

  public NovelNearestDocs() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(WordCoverageFirstGreeDi.class);
    job.setJobName("NovelNearestDocs");

    job.getConfiguration().setInt(DIMENSIONS_FIELD, dimensions);
    if (bands > 0) {
      job.getConfiguration().setInt(BANDS_FIELD, bands);
    }
    if (rows > 0) {
      job.getConfiguration().setInt(ROWS_FIELD, rows);
    }

    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(HashBandWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(HashBandWritable.class);
    job.setOutputValueClass(TextArrayWritable.class);

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
        .withDescription("Tfidf vectors").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Near documents").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Vectors' length").create(DIMENSIONS_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Number of bands").create(BANDS_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Number of rows").create(ROWS_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(DIMENSIONS_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(INPUT_OPTION);
    outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    dimensions = Integer.parseInt(cmdline.getOptionValue(DIMENSIONS_OPTION));

    bands = -1;
    if (cmdline.hasOption(BANDS_OPTION)) {
      bands = Integer.parseInt(cmdline.getOptionValue(BANDS_OPTION));
    }
    rows = -1;
    if (cmdline.hasOption(ROWS_OPTION)) {
      rows = Integer.parseInt(cmdline.getOptionValue(ROWS_OPTION));
    }

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - dimensions: " + dimensions);
    logger.info(" - output: " + outputPath);
    logger.info(" - bands: " + bands);
    logger.info(" - rows: " + rows);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new NovelNearestDocs(), args);
  }
}
