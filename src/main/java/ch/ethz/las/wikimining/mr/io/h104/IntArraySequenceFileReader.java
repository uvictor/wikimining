
package ch.ethz.las.wikimining.mr.io.h104;

import ch.ethz.las.wikimining.base.IntArrayWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Reads (HashBand bucket, doc ids) pairs from a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class IntArraySequenceFileReader
    extends SequenceFileProcessor<Integer, ArrayList<Integer>> {

  private HashSet<Integer> docIds;

  public IntArraySequenceFileReader(
      Path thePath, FileSystem theFs, JobConf theConfig) {
    super(thePath, theFs, theConfig);

    docIds = null;
  }

  @Override
  protected void processContent(FileStatus status) throws IOException {
    try (SequenceFile.Reader reader =
        new SequenceFile.Reader(fs, status.getPath(), config)) {
      IntWritable key = (IntWritable)
        ReflectionUtils.newInstance(reader.getKeyClass(), config);
      IntArrayWritable value = (IntArrayWritable)
        ReflectionUtils.newInstance(reader.getValueClass(), config);

      while (reader.next(key, value)) {
        if (docIds != null && !docIds.contains(key.get())) {
          continue;
        }

        ArrayList<Integer> edges = new ArrayList<>(value.get().length);
        for (Writable docIdWritable : value.get()) {
          IntWritable docId = (IntWritable) docIdWritable;
          edges.add(docId.get());
        }

        map.put(key.get(), edges);
      }
    }
  }

  public void setDocIds(HashSet<Integer> theDocIds) {
    docIds = theDocIds;
  }
}
