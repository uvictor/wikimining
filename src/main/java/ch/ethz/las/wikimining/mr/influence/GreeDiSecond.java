package ch.ethz.las.wikimining.mr.influence;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.io.SetupHelper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

/**
 * Computes the influence of a document as equation (5) from the paper,
 * from a novelty document index and a yearly word spread index, using MapReduce
 * and the GreeDi protocol. Second pass.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GreeDiSecond extends Configured implements Tool {

  private static final Logger logger =
      Logger.getLogger(GreeDiSecond.class);

  private static class Map extends
      Mapper<Text, VectorWritable, IntWritable, DocumentWithVectorWritable> {

    private static final IntWritable zero = new IntWritable(0);

    private Set<Integer> docsSubset;

    @Override
    public void setup(Context context) {
      try {
        Path path =
            new Path(context.getConfiguration().get(Fields.DOCS_SUBSET.get()));
        logger.info("Loading docs subset: " + path);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (!fs.exists(path)) {
          throw new RuntimeException(path + " does not exist!");
        }

        readDocsSubset(path, fs);
      } catch (IOException | RuntimeException e) {
        logger.fatal("Error loading docs subset ids!", e);
      }
    }

    @Override
    public void map(Text key, VectorWritable value, Mapper.Context context)
        throws IOException, InterruptedException {
      if (!docsSubset.contains(Integer.parseInt(key.toString()))) {
        return;
      }

      final DocumentWithVectorWritable outValue =
          new DocumentWithVectorWritable(key, value);

      // We use IntWritable only so that we can reuse GreeDiReducer.
      context.write(zero, outValue);
    }

    private void readDocsSubset(Path path, FileSystem fs) throws IOException {
      docsSubset = new HashSet<>();
      final FileStatus[] statuses = fs.listStatus(path);
      for (FileStatus status : statuses) {
        if (status.isDirectory()) {
          continue;
        }
        try (final FSDataInputStream in = fs.open(status.getPath());
            final Scanner s = new Scanner(in)) {
          while (s.hasNextInt()) {
            docsSubset.add(s.nextInt());
          }
        }
      }
    }
  }

  private String inputPath;
  private String outputPath;
  private String datesPath;
  private String wordSpreadPath;
  private String docsSubsetPath;
  private int selectCount;

  public GreeDiSecond() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(GreeDiSecond.class);
    job.setJobName(String.format("Influence-GreeDiSecond[%s]", selectCount));

    job.getConfiguration().set(Fields.DOC_DATES.get(), datesPath);
    job.getConfiguration().set(Fields.WORD_SPREAD.get(), wordSpreadPath);
    job.getConfiguration().set(Fields.DOCS_SUBSET.get(), docsSubsetPath);
    job.getConfiguration().setInt(Fields.SELECT_COUNT.get(), selectCount);

    job.setNumReduceTasks(1);

    SetupHelper.getInstance()
        .setSequenceInput(job, inputPath)
        .setTextOutput(job, outputPath);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DocumentWithVectorWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(GreeDiReducer.class);

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
        .withDescription("Selected articles").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Document dates").create(Fields.DOC_DATES.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Word spread yearly matrix")
        .create(Fields.WORD_SPREAD.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Selected docs subset").create(Fields.DOCS_SUBSET.get()));

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
        || !cmdline.hasOption(Fields.DOCS_SUBSET.get())
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
    docsSubsetPath = cmdline.getOptionValue(Fields.DOCS_SUBSET.get());

    selectCount = Integer.parseInt(cmdline.getOptionValue(Fields.SELECT_COUNT.get()));

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - dates: " + datesPath);
    logger.info(" - spread: " + wordSpreadPath);
    logger.info(" - docs: " + docsSubsetPath);
    logger.info(" - select: " + selectCount);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GreeDiSecond(), args);
  }
}
