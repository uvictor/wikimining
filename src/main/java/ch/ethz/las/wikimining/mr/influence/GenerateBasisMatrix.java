package ch.ethz.las.wikimining.mr.influence;

import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.utils.SetupHelper;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.random.RandomProjector;

/**
 * Generates a basis matrix to be used in LSH for the random projections.
 * This is not a per-se MapReduce, but merely a simple call to a Mahout method.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GenerateBasisMatrix extends Configured implements Tool {

  private static final Logger logger =
      Logger.getLogger(GenerateBasisMatrix.class);

  private static class Map extends
      Mapper<NullWritable, NullWritable, NullWritable, MatrixWritable> {

    @Override
    public void map(NullWritable key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      final int dimensions = context.getConfiguration().getInt(
          Fields.DIMENSIONS.get(), Defaults.DIMENSIONS.get());
      final int bands = context.getConfiguration().getInt(
          Fields.BANDS.get(), Defaults.BANDS.get());
      final int rows = context.getConfiguration().getInt(
          Fields.ROWS.get(), Defaults.ROWS.get());

      final Matrix basisMatrix =
          RandomProjector.generateBasisPlusMinusOne(bands * rows, dimensions);
      context.write(NullWritable.get(), new MatrixWritable(basisMatrix));
    }
  }

  private String outputPath;
  private int bands;
  private int rows;
  private int dimensions;

  public GenerateBasisMatrix() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(GenerateBasisMatrix.class);
    job.setJobName(String.format(
        "Influence-GenerateBasisMatrix[%s %s %s]", bands, rows, dimensions));

    if (bands != -1) {
      job.getConfiguration().setInt(Fields.BANDS.get(), bands);
    }
    if (rows != -1) {
      job.getConfiguration().setInt(Fields.ROWS.get(), rows);
    }
    job.getConfiguration().setInt(Fields.DIMENSIONS.get(), dimensions);

    job.setNumReduceTasks(0);

    SetupHelper.getInstance()
        .setNullInput(job)
        .setSequenceOutput(job, outputPath);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(MatrixWritable.class);

    job.setMapperClass(Map.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Basis matrix").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Vectors' length").create(Fields.DIMENSIONS.get()));
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

    if (!cmdline.hasOption(Fields.OUTPUT.get())
        || !cmdline.hasOption(Fields.DIMENSIONS.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());
    dimensions =
        Integer.parseInt(cmdline.getOptionValue(Fields.DIMENSIONS.get()));

    bands = -1;
    if (cmdline.hasOption(Fields.BANDS.get())) {
      bands = Integer.parseInt(cmdline.getOptionValue(Fields.BANDS.get()));
    }
    rows = -1;
    if (cmdline.hasOption(Fields.ROWS.get())) {
      rows = Integer.parseInt(cmdline.getOptionValue(Fields.ROWS.get()));
    }

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - dimensions: " + dimensions);
    logger.info(" - bands: " + bands);
    logger.info(" - rows: " + rows);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GenerateBasisMatrix(), args);
  }
}
