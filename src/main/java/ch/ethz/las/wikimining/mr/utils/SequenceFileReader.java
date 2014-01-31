/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.ethz.las.wikimining.mr.utils;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Reads (key, value) pairs from a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public abstract class SequenceFileReader<E, V> {

  protected final FileSystem fs;
  protected final Configuration config;
  protected final HashMap<E, V> map;

  private final Path path;

  public SequenceFileReader(
      Path thePath, FileSystem theFs, Configuration theConfig) {
    path = thePath;
    fs = theFs;
    config = theConfig;

    map = new HashMap<>();
  }

  public HashMap<E, V> read() throws IOException {
    if (!fs.exists(path)) {
      throw new IOException(path + " does not exist!");
    }

    final FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        continue;
      }
      if ("_SUCCESS".equals(status.getPath().getName())) {
        continue;
      }

      readContent(status);
    }

    return map;
  }

  protected abstract void readContent(FileStatus status) throws IOException;
}
