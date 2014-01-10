package ch.ethz.las.wikimining.mr;

import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.Set;
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
 * index, using MapReduce and the GreeDi protocol.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class DocumentWordCoverage extends Configured implements Tool {
  private static final int DEFAULT_PARTITION_COUNT = 16;

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String PARTITION_COUNT_OPTION = "partitions";

  private static final Logger logger
      = Logger.getLogger(DocumentWordCoverage.class);

  private String inputPath;
  private String outputPath;
  private int partitionCount;

  private static class Map extends
      Mapper<Text, VectorWritable, IntWritable, DocumentWithVectorWritable> {

    @Override
    public void map(Text key, VectorWritable value, Context context)
        throws IOException, InterruptedException {
      final int partition = Integer.parseInt(key.toString()) % DEFAULT_PARTITION_COUNT;
      final IntWritable outKey = new IntWritable(partition);

      final DocumentWithVectorWritable outValue =
          new DocumentWithVectorWritable(key, value);

      context.write(outKey, outValue);
    }
  }

  private static class Reduce extends Reducer<
      IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

    @Override
    public void reduce(IntWritable key,
        Iterable<DocumentWithVectorWritable> values, Context context)
        throws IOException, InterruptedException {
      final WordCoverageFromMahout objectiveFunction =
          new WordCoverageFromMahout(values);
      final SfoGreedyAlgorithm sfo = new SfoGreedyLazy(objectiveFunction);
      Set<Integer> selected =
          sfo.run(objectiveFunction.getAllDocIds(), DEFAULT_PARTITION_COUNT);

      for (Integer docId : selected) {
        IntWritable outValue = new IntWritable(docId);
        context.write(NullWritable.get(), outValue);
      }
    }
  }

  public DocumentWordCoverage() { }

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(DocumentWordCoverage.class);
    job.setJobName(String.format("DocumentWordCoverage[%s]", DEFAULT_PARTITION_COUNT));

    job.setNumReduceTasks(DEFAULT_PARTITION_COUNT);

    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    TextOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
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

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION) ||
        !cmdline.hasOption(PARTITION_COUNT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(INPUT_OPTION);
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

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DocumentWordCoverage(), args);
  }
}
