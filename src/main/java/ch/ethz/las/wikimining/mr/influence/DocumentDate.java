package ch.ethz.las.wikimining.mr.influence;

import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.utils.PageTypeChecker;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import java.io.IOException;
import java.util.Calendar;
import javax.xml.bind.DatatypeConverter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.tools.ant.filters.StringInputStream;

/**
 * Tool for extracting a date for each @{WikipediaPage}.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class DocumentDate extends Configured implements Tool {

  private static final Logger logger = Logger.getLogger(DocumentDate.class);

  private static class MyMapper extends
      Mapper<LongWritable, WikipediaPage, IntWritable, IntWritable> {

    @Override
    public void map(LongWritable key, WikipediaPage doc, Context context)
        throws IOException, InterruptedException {
      if (!PageTypeChecker.isArticle(doc, context)) {
        return;
      }

      final IntWritable id = new IntWritable(Integer.parseInt(doc.getDocid()));
      try {
        final Calendar date = getPageTimeStamp(doc.getRawXML());
        final IntWritable dateOutput = new IntWritable(
            date.get(Calendar.YEAR) * 1000 + date.get(Calendar.DAY_OF_YEAR));

        context.write(id, dateOutput);
      } catch (XMLStreamException ex) {
        logger.warn("Couldn't get the XML date for docid " + id, ex);
      }
    }

    private Calendar getPageTimeStamp(String xml) throws XMLStreamException {
    String timestamp = null;
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader reader =
        factory.createXMLStreamReader(new StringInputStream(xml));

    while(reader.hasNext()){
      int event = reader.next();

      if (event == XMLStreamConstants.CHARACTERS) {
        timestamp = reader.getText();
      } else if (event == XMLStreamConstants.END_ELEMENT) {
        if ("timestamp".equals(reader.getLocalName())) {
          break;
        }
      }
    }

    return DatatypeConverter.parseDateTime(timestamp);
    }
  }

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(Fields.INPUT.get()));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output location").create(Fields.INPUT.get()));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(Fields.INPUT.get()) || !cmdline.hasOption(Fields.INPUT.get())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(Fields.INPUT.get());
    String outputPath = cmdline.getOptionValue(Fields.INPUT.get());

    Job job = Job.getInstance(getConf());
    job.setJarByClass(DocumentDate.class);
    job.setJobName(String.format("Influence-Document Date[%s: %s, %s: %s]",
        Fields.INPUT.get(), inputPath, Fields.INPUT.get(), outputPath));

    logger.info("Tool name: " + this.getClass().getName());
    logger.info(" - input path: " + inputPath);
    logger.info(" - output path: " + outputPath);

    job.setNumReduceTasks(0);

    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(WikipediaPageInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  public DocumentDate() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DocumentDate(), args);
  }
}
