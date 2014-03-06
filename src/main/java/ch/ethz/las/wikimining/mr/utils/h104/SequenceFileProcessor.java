package ch.ethz.las.wikimining.mr.utils.h104;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * Reads (key, value) pairs from a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public abstract class SequenceFileProcessor<E, V> {

  protected final FileSystem fs;
  protected final JobConf config;
  protected final HashMap<E, V> map;

  private final Path path;

  public SequenceFileProcessor(
      Path thePath, FileSystem theFs, JobConf theConfig) {
    path = thePath;
    fs = theFs;
    config = theConfig;

    map = new LinkedHashMap<>();
  }

  public HashMap<E, V> processFile() throws IOException {
    if (!fs.exists(path)) {
      throw new IOException(path + " does not exist!");
    }

    final FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status : statuses) {
      if (status.isDir()) {
        continue;
      }
      if ("_SUCCESS".equals(status.getPath().getName())) {
        continue;
      }

      processContent(status);
    }

    return map;
  }

  protected abstract void processContent(FileStatus status) throws IOException;
}
