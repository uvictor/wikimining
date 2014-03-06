
package ch.ethz.las.wikimining.mr.utils.h104;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Write (Text id, Tf-idf vector) pairs to a sequence file.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TextVectorSequenceFileWriter
    extends SequenceFileProcessor<Integer, Vector> {
  public TextVectorSequenceFileWriter(HashMap<Integer, Vector> theMap,
      Path thePath, FileSystem theFs, JobConf theConfig) {
    super(thePath, theFs, theConfig);
    map.putAll(theMap);
  }

  @Override
  protected void processContent(FileStatus status) throws IOException {
    try (SequenceFile.Writer writer = new SequenceFile.Writer(
        fs, config, status.getPath(), Text.class, VectorWritable.class)) {
      for (Map.Entry<Integer, Vector> entry : map.entrySet()) {
        writer.append(new Text(entry.getKey().toString()),
            new VectorWritable(entry.getValue()));
      }
    }
  }
}
