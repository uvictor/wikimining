package ch.ethz.las.wikimining.mr.utils.h104;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;

/**
 * Reads a Mahout matrix from a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class MatrixSequenceFileReader {
  protected final FileSystem fs;
  protected final JobConf config;

  private final Path path;

  public MatrixSequenceFileReader(
      Path thePath, FileSystem theFs, JobConf theConfig) {
    path = thePath;
    fs = theFs;
    config = theConfig;
  }

  public Matrix read() throws IOException {
    if (!fs.exists(path)) {
      throw new IOException(path + " does not exist!");
    }

    return readContent();
  }

  protected Matrix readContent() throws IOException {
    NullWritable key;
    MatrixWritable value;

    try (SequenceFile.Reader reader =
        new SequenceFile.Reader(fs, path, config)) {
      key = (NullWritable)
        ReflectionUtils.newInstance(reader.getKeyClass(), config);
      value = (MatrixWritable)
        ReflectionUtils.newInstance(reader.getValueClass(), config);

      reader.next(key, value);
    }

    return value.get();
  }
}
