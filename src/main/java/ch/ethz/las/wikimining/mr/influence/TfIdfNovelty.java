package ch.ethz.las.wikimining.mr.influence;

import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVector;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.base.HashBandWritable;
import ch.ethz.las.wikimining.mr.io.IntegerSequenceFileReader;
import ch.ethz.las.wikimining.mr.io.MatrixSequenceFileReader;
import ch.ethz.las.wikimining.mr.io.SequenceFileReader;
import ch.ethz.las.wikimining.mr.io.SetupHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.mahout.math.Vector.Element;
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

  private static enum Records {

    TOTAL,
    POTENTIALLY_IGNORED
  };

  private static class Map extends Mapper<
      Text, VectorWritable, HashBandWritable, DocumentWithVectorWritable> {

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
        context.write(new HashBandWritable(hash, band),
            new DocumentWithVectorWritable(docId, value));
      }
    }
  }

  /**
   * TODO(uvictor): Important!: remove duplicates !!
   * TODO(uvictor): consider selecting the nearest neighbour more accurately
   * (ie, use tighter bounds).
   *
   * Currently, documents that don't have nearest neighbour in the past get
   * ignored (we don't output any vector for them and they will not get
   * considered).
   */
  private static class Reduce extends Reducer<
      HashBandWritable, DocumentWithVectorWritable, Text, VectorWritable> {

    private HashSet<Integer> parsedDocIds;
    private HashMap<Integer, Integer> docDates;

    @Override
    public void setup(Context context) {
      parsedDocIds = new HashSet<>();

      try {
        Path datesPath =
            new Path(context.getConfiguration().get(Fields.DOC_DATES.get()));
        logger.info("Loading doc dates: " + datesPath);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        final SequenceFileReader datesReader = new IntegerSequenceFileReader(
            datesPath, fs, context.getConfiguration());
        docDates = datesReader.read();
      } catch (IOException e) {
        logger.fatal("Error loading doc dates!", e);
      }
      logger.info("Loaded " + docDates.size() + " doc dates.");
    }

    @Override
    public void reduce(HashBandWritable key,
        Iterable<DocumentWithVectorWritable> docBucket, Context context)
        throws IOException, InterruptedException {
      final boolean ignoreDocs =
          context.getConfiguration().getBoolean(Fields.IGNORE.get(), false);
      final ArrayList<DocumentWithVector> docs = new ArrayList<>();
      for (DocumentWithVectorWritable doc : docBucket) {
        final int docId = Integer.parseInt(doc.getId().toString());
        docs.add(new DocumentWithVector(docId, doc.getVector().get()));
      }

      if (docs.size() <= 1) {
        // No near neighbours.
        context.getCounter(Records.POTENTIALLY_IGNORED).increment(1);
        if (!ignoreDocs) {
          context.write(new Text(Integer.toString(docs.get(0).getId())),
              new VectorWritable(docs.get(0).getVector()));
        }
        return;
      }

      // TODO(uvictor): sort the documents on date and do binary search.
      // TODO(uvictor): consider different strategies for selecting the NN:
      // first, random, closeset, closest date etc.
      for (DocumentWithVector current : docs) {
        final int currentId = current.getId();
        if (parsedDocIds.contains(currentId)) {
          continue;
        }

        for (DocumentWithVector before : docs) {
          final int beforeId = before.getId();
          if (currentId == beforeId) {
            continue;
          }
          if (docDates.get(beforeId) > docDates.get(currentId)) {
            continue;
          }

          final Vector currentVector = current.getVector();
          for (Element element : currentVector.nonZeroes()) {
            final int index = element.index();
            final double result =
                element.get() - before.getVector().getQuick(index);
            if (result > 0) {
              currentVector.setQuick(index, result);
            }
          }

          context.write(new Text(Integer.toString(currentId)),
              new VectorWritable(currentVector));
          parsedDocIds.add(currentId);
          break;
        }

        if (!parsedDocIds.contains((currentId))) {
          context.getCounter(Records.POTENTIALLY_IGNORED).increment(1);
          if (!ignoreDocs) {
            context.write(new Text(Integer.toString(currentId)),
                new VectorWritable(current.getVector()));
          }
        }
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

    Job job = Job.getInstance(getConf());
    job.setJarByClass(TfIdfNovelty.class);
    job.setJobName("Influence-TfIdfNovelty");

    job.getConfiguration().set(Fields.BASIS.get(), basisPath);
    job.getConfiguration().set(Fields.DOC_DATES.get(), datesPath);
    job.getConfiguration().setBoolean(Fields.IGNORE.get(), ignoreDocs);
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
    job.setMapOutputValueClass(DocumentWithVectorWritable.class);
    job.setOutputKeyClass(Text.class);
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
