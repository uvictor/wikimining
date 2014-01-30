/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.ethz.las.wikimining.mr;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author uvictor
 */
public class DocDatesReader {

  private final Path path;
  private final FileSystem fs;
  private final Configuration config;
  private final HashMap<Integer, Integer> docDates;

  public DocDatesReader(
      Path thePath, FileSystem theFs, Configuration theConfig) {
    path = thePath;
    fs = theFs;
    config = theConfig;

    docDates = new HashMap<>();
  }

  public void read()
        throws IOException {
    final FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        continue;
      }
      if ("_SUCCESS".equals(status.getPath().getName())) {
        continue;
      }

      try (SequenceFile.Reader reader =
          new SequenceFile.Reader(fs, status.getPath(), config)) {
        IntWritable key = (IntWritable)
          ReflectionUtils.newInstance(reader.getKeyClass(), config);
        IntWritable value = (IntWritable)
          ReflectionUtils.newInstance(reader.getValueClass(), config);
        while (reader.next(key, value)) {
          docDates.put(key.get(), value.get());
        }
      }
    }
  }

  public HashMap<Integer, Integer> getDocDates() {
    return docDates;
  }
}
