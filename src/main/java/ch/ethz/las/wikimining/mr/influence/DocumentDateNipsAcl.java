package ch.ethz.las.wikimining.mr.influence;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

/**
 * Tool for extracting a date for each Nips paper. The date is actually
 * represented by the first two digits of the key.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class DocumentDateNipsAcl extends Configured implements Tool {

  private static final Logger logger = Logger.getLogger(DocumentDateNipsAcl.class);

  private static class Map extends
      Mapper<Text, VectorWritable, IntWritable, IntWritable> {

    @Override
    public void map(Text key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      final int intId = Integer.parseInt(key.toString());
      final IntWritable id = new IntWritable(intId);
      final IntWritable date = new IntWritable((intId / 10000) % 100);

      context.write(id, date);
    }
  }

  public DocumentDateNipsAcl() { }

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Tfidf").create(Fields.INPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Output location").create(Fields.OUTPUT.get()));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(Fields.INPUT.get())
        || !cmdline.hasOption(Fields.OUTPUT.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    String outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());

    Job job = Job.getInstance(getConf());
    job.setJarByClass(DocumentDateNipsAcl.class);
    job.setJobName(String.format("Influence-Document Date[%s: %s, %s: %s]",
        Fields.INPUT.get(), inputPath, Fields.OUTPUT.get(), outputPath));

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input path: " + inputPath);
    logger.info(" - output path: " + outputPath);

    job.setNumReduceTasks(0);

    SetupHelper.getInstance()
        .setSequenceInput(job, inputPath)
        .setSequenceOutput(job, outputPath);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DocumentDateNipsAcl(), args);
  }
}
