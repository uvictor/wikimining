package ch.ethz.las.wikimining.mr.utils.h104;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Reads consecutive (Text word, Long count) pairs from a sequence file and
 * changes them to (Integer #, Long count), ignoring the Text of the word.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class FakeIntLongSequenceFileReader extends IntLongSequenceFileReader {

  private int count;

  public FakeIntLongSequenceFileReader(
      Path thePath, FileSystem theFs, JobConf theConfig) {
    super(thePath, theFs, theConfig);
    count = 0;
  }

  @Override
  protected void processContent(FileStatus status) throws IOException {
    try (SequenceFile.Reader reader =
        new SequenceFile.Reader(fs, status.getPath(), config)) {
      Text key = (Text)
        ReflectionUtils.newInstance(reader.getKeyClass(), config);
      LongWritable value = (LongWritable)
        ReflectionUtils.newInstance(reader.getValueClass(), config);
      while (reader.next(key, value)) {
        map.put(count, value.get());
        count++;
      }
    }
  }
}
