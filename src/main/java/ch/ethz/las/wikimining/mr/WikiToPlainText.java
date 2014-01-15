package ch.ethz.las.wikimining.mr;

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
 * Tool for repacking Wikipedia XML dumps into <code>SequenceFiles</code>.
 * <p>
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikiToPlainText extends Configured implements Tool {

  private static final Logger logger = Logger.getLogger(WikiToPlainText.class);

  private static enum Records {

    TOTAL
  };

  private static class MyMapper extends
      Mapper<LongWritable, WikipediaPage, Text, Text> {

    @Override
    public void map(LongWritable key, WikipediaPage doc, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);
      String id = doc.getDocid();
      if (id != null) {
          context.write(new Text(id), new Text(doc.getContent()));
      }
    }
  }

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String COMPRESSION_TYPE_OPTION = "compression_type";
  private static final String LANGUAGE_OPTION = "wiki_language";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output location").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("block|record|none").hasArg()
        .withDescription("compression type").create(COMPRESSION_TYPE_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|de").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(COMPRESSION_TYPE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    String compressionType = cmdline.getOptionValue(COMPRESSION_TYPE_OPTION);

    if (!"block".equals(compressionType) && !"record".equals(compressionType) && !"none".equals(compressionType)) {
      System.err.println("Error: \"" + compressionType + "\" unknown compression type!");
      return -1;
    }

    String language = null;
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if (language.length() != 2) {
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    // this is the default block size
    int blocksize = 1000000;

    Job job = Job.getInstance(getConf());
    job.setJarByClass(WikiToPlainText.class);
    job.setJobName(String.format("RepackWikipedia[%s: %s, %s: %s, %s: %s, %s: %s]",
        INPUT_OPTION, inputPath, OUTPUT_OPTION, outputPath, COMPRESSION_TYPE_OPTION,
        compressionType, LANGUAGE_OPTION, language));

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - XML dump file: " + inputPath);
    logger.info(" - output path: " + outputPath);
    logger.info(" - compression type: " + compressionType);
    logger.info(" - language: " + language);

    if ("block".equals(compressionType)) {
      logger.info(" - block size: " + blocksize);
    }

    job.setNumReduceTasks(0);

    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

    if ("none".equals(compressionType)) {
      SequenceFileOutputFormat.setCompressOutput(job, false);
    } else {
      SequenceFileOutputFormat.setCompressOutput(job, true);

      if ("record".equals(compressionType)) {
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
      } else {
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.getConfiguration().setInt("io.seqfile.compress.blocksize", blocksize);
      }
    }

    if (language != null) {
      job.getConfiguration().set("wiki.language", language);
    }

    job.setInputFormatClass(WikipediaPageInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  public WikiToPlainText() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new WikiToPlainText(), args);
  }
}
