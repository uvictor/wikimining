
package ch.ethz.las.wikimining.mr.io.h104;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Reads (Text id, Tf-idf vector) pairs from a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TextVectorSequenceFileReader
    extends SequenceFileProcessor<Integer, Vector> {

  public TextVectorSequenceFileReader(
      Path thePath, FileSystem theFs, JobConf theConfig) {
    super(thePath, theFs, theConfig);
  }

  @Override
  protected void processContent(FileStatus status) throws IOException {
    try (SequenceFile.Reader reader =
        new SequenceFile.Reader(fs, status.getPath(), config)) {
      Text key = (Text)
        ReflectionUtils.newInstance(reader.getKeyClass(), config);
      VectorWritable value = (VectorWritable)
        ReflectionUtils.newInstance(reader.getValueClass(), config);
      while (reader.next(key, value)) {
        map.put(Integer.parseInt(key.toString()), value.get());
      }
    }
  }
}
