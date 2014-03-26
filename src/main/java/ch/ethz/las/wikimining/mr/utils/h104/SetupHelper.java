
package ch.ethz.las.wikimining.mr.utils.h104;

import ch.ethz.las.wikimining.mr.base.Defaults;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

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

  public SetupHelper setTextInput(JobConf config, String outputPath) {
    TextInputFormat.addInputPath(config, new Path(outputPath));
    config.setInputFormat(TextInputFormat.class);

    return this;
  }

  public SetupHelper setTextOutput(JobConf config, String outputPath) {
    TextOutputFormat.setOutputPath(config, new Path(outputPath));
    config.setOutputFormat(TextOutputFormat.class);

    return this;
  }

  public SetupHelper setSequenceInput(JobConf config, String inputPath)
      throws IOException {
    SequenceFileInputFormat.addInputPath(config, new Path(inputPath));
    config.setInputFormat(SequenceFileInputFormat.class);

    return this;
  }

  public SetupHelper setSequenceOutput(JobConf config, String outputPath) {
    SequenceFileOutputFormat.setOutputPath(config, new Path(outputPath));
    SequenceFileOutputFormat
        .setOutputCompressionType(config, SequenceFile.CompressionType.BLOCK);
    config.setInt("io.seqfile.compress.blocksize", Defaults.BLOCK_SIZE.get());

    config.setOutputFormat(SequenceFileOutputFormat.class);

    return this;
  }
}
