package ch.ethz.las.wikimining.mr.io.h104;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Reads (Integer id, Integer date) pairs from a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class IntegerSequenceFileReader
    extends SequenceFileProcessor<Integer, Integer> {

  public IntegerSequenceFileReader(
      Path thePath, FileSystem theFs, JobConf theConfig) {
    super(thePath, theFs, theConfig);
  }

  @Override
  protected void processContent(FileStatus status) throws IOException {
    try (SequenceFile.Reader reader =
        new SequenceFile.Reader(fs, status.getPath(), config)) {
      IntWritable key = (IntWritable)
        ReflectionUtils.newInstance(reader.getKeyClass(), config);
      IntWritable value = (IntWritable)
        ReflectionUtils.newInstance(reader.getValueClass(), config);
      while (reader.next(key, value)) {
        map.put(key.get(), value.get());
      }
    }
  }
}
