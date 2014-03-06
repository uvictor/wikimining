
package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.HashBandWritable;
import ch.ethz.las.wikimining.mr.base.IntArrayWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Outputs the buckets themselves so that we can use them as part of the
 * objective function.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfNoveltyIdentityReducer extends MapReduceBase
    implements Reducer<HashBandWritable, DocumentWithVectorWritable,
    HashBandWritable, IntArrayWritable> {

  private static final Logger logger =
      Logger.getLogger(TfIdfNoveltyIdentityReducer.class);

  @Override
  public void reduce(
      HashBandWritable key, Iterator<DocumentWithVectorWritable> docBucket,
      OutputCollector<HashBandWritable, IntArrayWritable> output,
      Reporter reporter) throws IOException {
    reporter.getCounter(TfIdfNovelty.Records.TOTAL).increment(1);

    ArrayList<IntWritable> bucket = new ArrayList<>();
    while(docBucket.hasNext()) {
      bucket.add(new IntWritable(
          Integer.parseInt(docBucket.next().getId().toString())));
    }

    output
        .collect(key, new IntArrayWritable(bucket.toArray(new IntWritable[0])));
  }
}
