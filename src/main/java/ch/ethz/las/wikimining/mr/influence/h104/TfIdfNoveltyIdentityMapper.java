
package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.base.HashBandWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

/**
 * Outputs all documents in the same bucket so that we can find the exact
 * nearest neighbours.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfNoveltyIdentityMapper extends MapReduceBase implements Mapper<
    Text, VectorWritable, HashBandWritable, DocumentWithVectorWritable> {

  private static final Logger logger =
      Logger.getLogger(TfIdfNoveltyIdentityMapper.class);

  @Override
  public void map(Text docId, VectorWritable value,
      OutputCollector<HashBandWritable, DocumentWithVectorWritable> output,
      Reporter reporter) throws IOException {
    reporter.getCounter(TfIdfNovelty.Records.TOTAL).increment(1);

    // Use (0, 0) for the HashBandWritable so we get all document in the same
    // bucket.
    output.collect(new HashBandWritable(0, 0),
        new DocumentWithVectorWritable(docId, value));
  }
}
