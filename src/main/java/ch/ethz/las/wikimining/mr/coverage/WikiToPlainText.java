package ch.ethz.las.wikimining.mr.coverage;

import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.utils.PageTypeChecker;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for repacking Wikipedia XML dumps into SequenceFiles.
 * <p>
 * @author Jimmy Lin
 * @author Peter Exner
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WikiToPlainText extends Configured implements Tool {

  private static final Logger logger = Logger.getLogger(WikiToPlainText.class);

  public static class Map extends
      Mapper<LongWritable, WikipediaPage, Text, Text> {

    @Override
    public void map(LongWritable key, WikipediaPage doc, Context context)
        throws IOException, InterruptedException {
      // We minimize the number of calls to doc.getContent() as it involves
      // parsing the page XML for each call.
      final String stringContent = PageTypeChecker.isArticle(doc, context);
      if (stringContent == null) {
        return;
      }

      // TODO(uvictor): remove hack for Cloud9's WikipediaPage.getContent()
      final Text docContent;
      try {
        docContent = new Text(stringContent);
      } catch (NullPointerException e) {
        logger.error("WikipediaPage.getContent() NullPointerExcetion", e);
        return;
      }

      context.write(new Text(doc.getDocid()), docContent);
    }
  }

  private String inputPath;
  private String outputPath;
  private String compressionType;
  private String language;

  @Override
  public int run(String[] args) throws Exception {
    final int ret = parseArgs(args);
    if (ret < 0) {
      return ret;
    }

    // this is the default block size
    final int defaultBlockSize = 1000000;

    Job job = Job.getInstance(getConf());
    job.setJarByClass(WikiToPlainText.class);
    job.setJobName(String.format(
        "WikiToPlainText[%s: %s, %s: %s, %s: %s, %s: %s]", Fields.INPUT.get(),
        inputPath, Fields.OUTPUT.get(), outputPath, Fields.COMPRESSION.get(),
        compressionType, Fields.LANGUAGE.get(), language));

    if ("block".equals(compressionType)) {
      logger.info(" - block size: " + defaultBlockSize);
    }

    job.setNumReduceTasks(0);

    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

    if ("none".equals(compressionType)) {
      SequenceFileOutputFormat.setCompressOutput(job, false);
    } else {
      SequenceFileOutputFormat.setCompressOutput(job, true);

      if ("record".equals(compressionType)) {
        SequenceFileOutputFormat
            .setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
      } else {
        SequenceFileOutputFormat
            .setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.getConfiguration()
            .setInt("io.seqfile.compress.blocksize", defaultBlockSize);
      }
    }

    if (language != null) {
      job.getConfiguration().set("wiki.language", language);
    }

    job.setInputFormatClass(WikipediaPageInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

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
        .withDescription("XML dump file").create(Fields.INPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output location").create(Fields.OUTPUT.get()));
    options.addOption(OptionBuilder.withArgName("block|record|none").hasArg()
        .withDescription("compression type").create(Fields.COMPRESSION.get()));
    options.addOption(OptionBuilder.withArgName("en|sv|de").hasArg()
        .withDescription("two-letter language code").create(Fields.LANGUAGE.get()));

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
        || !cmdline.hasOption(Fields.COMPRESSION.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    outputPath = cmdline.getOptionValue(Fields.OUTPUT.get());
    compressionType = cmdline.getOptionValue(Fields.COMPRESSION.get());

    if (!"block".equals(compressionType) && !"record".equals(compressionType) && !"none".equals(compressionType)) {
      System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
      return -1;
    }

    language = null;
    if (cmdline.hasOption(Fields.LANGUAGE.get())) {
      language = cmdline.getOptionValue(Fields.LANGUAGE.get());
      if (language.length() != 2) {
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - XML dump file: " + inputPath);
    logger.info(" - output path: " + outputPath);
    logger.info(" - compression type: " + compressionType);
    logger.info(" - language: " + language);

    return 0;
  }

  public WikiToPlainText() { }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new WikiToPlainText(), args);
  }
}
