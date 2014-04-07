package ch.ethz.las.wikimining.mr.io.h104;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.VectorWritable;

/**
 * Reads Integer id keys from a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TextVectorKeySequenceFileReader
    extends SequenceFileProcessor<Integer, Integer> {

  public TextVectorKeySequenceFileReader(
      Path thePath, FileSystem theFs, JobConf theConfig) {
    super(thePath, theFs, theConfig);
  }

  @Override
  protected void processContent(FileStatus status) throws IOException {
    try (final SequenceFile.Reader reader =
        new SequenceFile.Reader(fs, status.getPath(), config)) {
      final Text key = (Text)
        ReflectionUtils.newInstance(reader.getKeyClass(), config);
      final VectorWritable value = (VectorWritable)
        ReflectionUtils.newInstance(reader.getValueClass(), config);
      while (reader.next(key, value)) {
        map.put(Integer.parseInt(key.toString()), null);
      }
    }
  }
}
