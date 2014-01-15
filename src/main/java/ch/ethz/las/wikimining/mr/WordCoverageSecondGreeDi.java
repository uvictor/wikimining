package ch.ethz.las.wikimining.mr;

import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

/**
 * Computes the word coverage as equation (1) from the paper, from a Mahout
 * index, using MapReduce and the GreeDi protocol. First pass.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WordCoverageSecondGreeDi extends Configured implements Tool {

  private static class Map extends
      Mapper<Text, VectorWritable, NullWritable, DocumentWithVectorWritable> {

    private Set<Integer> docsSubset;

    @Override
    public void setup(Context context) {
      try {
        Path path = new Path(context.getConfiguration().get(DOCS_SUBSET_FIELD));
        logger.info("Loading docs subset: " + path);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (!fs.exists(path)) {
          throw new RuntimeException(path + " does not exist!");
        }

        readDocsSubset(path, fs);
      } catch (IOException | RuntimeException e) {
        throw new RuntimeException("Error loading docs subset ids!");
      }
    }

    @Override
    public void map(Text key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      if (!docsSubset.contains(Integer.parseInt(key.toString()))) {
        return;
      }

      final DocumentWithVectorWritable outValue =
          new DocumentWithVectorWritable(key, value);

      context.write(NullWritable.get(), outValue);
    }

    private void readDocsSubset(Path path, FileSystem fs) throws IOException {
      try (FSDataInputStream in = fs.open(path.suffix("/part-r-00000")); Scanner s = new Scanner(in)) {
        docsSubset = new HashSet<>();
        while (s.hasNextInt()) {
          docsSubset.add(s.nextInt());
        }
      }
    }
  }

  private static class Reduce extends Reducer<
      NullWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

    @Override
    public void reduce(NullWritable key,
        Iterable<DocumentWithVectorWritable> values, Context context)
        throws IOException, InterruptedException {
      final WordCoverageFromMahout objectiveFunction =
          new WordCoverageFromMahout(values);
      final SfoGreedyAlgorithm sfo = new SfoGreedyLazy(objectiveFunction);
      final int partitionCount = context.getConfiguration()
          .getInt(PARTITION_COUNT_FIELD, DEFAULT_PARTITION_COUNT);
      Set<Integer> selected =
          sfo.run(objectiveFunction.getAllDocIds(), partitionCount);

      for (Integer docId : selected) {
        IntWritable outValue = new IntWritable(docId);
        context.write(NullWritable.get(), outValue);
      }
    }
  }

  private static final int DEFAULT_PARTITION_COUNT = 16;
  private static final String DOCS_SUBSET_FIELD = "DocsSubsetField";
  private static final String PARTITION_COUNT_FIELD = "PartitionCountField";

  private static final String INPUT_OPTION = "input";
  private static final String DOCS_SUBSET_OPTION = "docs";
  private static final String OUTPUT_OPTION = "output";
  private static final String PARTITION_COUNT_OPTION = "partitions";

  private static final Logger logger =
      Logger.getLogger(WordCoverageSecondGreeDi.class);

  private String inputPath;
  private String docsSubsetPath;
  private String outputPath;
  private int partitionCount;

  public WordCoverageSecondGreeDi() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(WordCoverageFirstGreeDi.class);
    job.setJobName(String.format("DocumentWordCoverage[%s]", partitionCount));

    job.getConfiguration().set(DOCS_SUBSET_FIELD, docsSubsetPath);
    job.getConfiguration().setInt(PARTITION_COUNT_FIELD, partitionCount);

    job.setNumReduceTasks(1);

    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    TextOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(DocumentWithVectorWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);

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
        .withDescription("Selected docs subset").create(DOCS_SUBSET_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("Selected articles").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("integer").hasArg()
        .withDescription("Partition count").create(PARTITION_COUNT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(DOCS_SUBSET_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(INPUT_OPTION);
    docsSubsetPath = cmdline.getOptionValue(DOCS_SUBSET_OPTION);
    outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

    partitionCount = DEFAULT_PARTITION_COUNT;
    if (cmdline.hasOption(PARTITION_COUNT_OPTION)) {
      partitionCount =
          Integer.parseInt(cmdline.getOptionValue(PARTITION_COUNT_OPTION));
      if(partitionCount <= 0){
        System.err.println(
            "Error: \"" + partitionCount + "\" has to be positive!");
        return -1;
      }
    }

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input: " + inputPath);
    logger.info(" - docs: " + docsSubsetPath);
    logger.info(" - output: " + outputPath);
    logger.info(" - patitions: " + partitionCount);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new WordCoverageSecondGreeDi(), args);
  }
}
