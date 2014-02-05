
package ch.ethz.las.wikimining.mr.utils;

import ch.ethz.las.wikimining.mr.base.Defaults;
import edu.umd.cloud9.mapreduce.NullInputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Helper for common MapReduce setup operations.
 *
 * @author Victor Ungureanu
 */
public class SetupHelper {
  private static SetupHelper instance = null;

  public static SetupHelper getInstance() {
    if (instance == null) {
      instance = new SetupHelper();
    }

    return instance;
  }

  private SetupHelper() { }

  public SetupHelper setTextOutput(Job job, String outputPath) {
    TextOutputFormat.setOutputPath(job, new Path(outputPath));
    job.setOutputFormatClass(TextOutputFormat.class);

    return this;
  }

  public SetupHelper setSequenceInput(Job job, String inputPath)
      throws IOException {
    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    job.setInputFormatClass(SequenceFileInputFormat.class);

    return this;
  }

  public SetupHelper setSequenceOutput(Job job, String outputPath) {
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    SequenceFileOutputFormat
        .setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    job.getConfiguration()
        .setInt("io.seqfile.compress.blocksize", Defaults.BLOCK_SIZE.get());

    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    return this;
  }

  public SetupHelper setNullInput(Job job) {
    job.setInputFormatClass(NullInputFormat.class);

    return this;
  }
}
