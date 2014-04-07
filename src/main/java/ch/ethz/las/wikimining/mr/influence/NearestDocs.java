package ch.ethz.las.wikimining.mr.influence;

import ch.ethz.las.wikimining.base.Defaults;
import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.base.HashBandWritable;
import ch.ethz.las.wikimining.base.TextArrayWritable;
import ch.ethz.las.wikimining.mr.coverage.GreeDiFirst;
import ch.ethz.las.wikimining.mr.io.MatrixSequenceFileReader;
import ch.ethz.las.wikimining.mr.io.SetupHelper;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.VectorFunction;

/**
 * Finds k-nearest documents, for each document.
 * Uses Locality Sensitive Hashing with Random Projections (for cosine
 * similarity).
 *
 * @deprecated not used
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class NearestDocs extends Configured implements Tool {

  private static enum Records {

    TOTAL
  };

  private static class Map extends
      Mapper<Text, VectorWritable, HashBandWritable, Text> {

    private Matrix basisMatrix;

    @Override
    public void setup(Context context) {
      try {
        final Path basisPath =
            new Path(context.getConfiguration().get(Fields.BASIS.get()));
        FileSystem fs = FileSystem.get(context.getConfiguration());

        final MatrixSequenceFileReader basisReader =
            new MatrixSequenceFileReader(basisPath.suffix("/part-m-00000"),
                fs, context.getConfiguration());
        basisMatrix = basisReader.read();
      } catch (IOException e) {
        logger.fatal("Error loading basis matrix!", e);
      }
    }

    @Override
    public void map(Text docId, VectorWritable value, Mapper.Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      final int bandCount = context.getConfiguration()
          .getInt(Fields.BANDS.get(), Defaults.BANDS.get());
      final int rowCount = context.getConfiguration()
          .getInt(Fields.ROWS.get(), Defaults.ROWS.get());
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

  private static final Logger logger = Logger.getLogger(GreeDiFirst.class);

  private String inputPath;
  private String outputPath;
  private String basisPath;
  private int bands;
  private int rows;

  public NearestDocs() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(GreeDiFirst.class);
    job.setJobName("Influence-NearestDocs");

    job.getConfiguration().set(Fields.BASIS.get(), basisPath);
    if (bands > 0) {
      job.getConfiguration().setInt(Fields.BANDS.get(), bands);
    }
    if (rows > 0) {
      job.getConfiguration().setInt(Fields.ROWS.get(), rows);
    }

    SetupHelper.getInstance()
        .setSequenceInput(job, inputPath)
        .setSequenceOutput(job, outputPath);

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
        .withDescription("Tfidf vectors").create(Fields.INPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Near documents").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Vectors' length").create(Fields.BASIS.get()));
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
        || !cmdline.hasOption(Fields.BASIS.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());
    basisPath = cmdline.getOptionValue(Fields.BASIS.get());

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
    logger.info(" - basisPath: " + basisPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - bands: " + bands);
    logger.info(" - rows: " + rows);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new NearestDocs(), args);
  }
}
