
package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.mr.base.DocumentWithVector;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.base.HashBandWritable;
import ch.ethz.las.wikimining.mr.base.NearestNeighbourCollection;
import ch.ethz.las.wikimining.mr.base.SquaredNearestNeighbour;
import ch.ethz.las.wikimining.mr.io.h104.IntegerSequenceFileReader;
import ch.ethz.las.wikimining.mr.io.h104.SequenceFileProcessor;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleDoubleFunction;

/**
 * Reducer to compute the novelty tf-idf according to equation 3.
 *
 * Finds the k-nearest documents from the past, for each document.
 *
 * TODO(uvictor): consider selecting the nearest neighbour more accurately
 * (ie. use tighter bounds).
 *
 * One can ignore documents that don't have a nearest neighbour in the past
 * (we don't output any vector for them and they will not get considered), by
 * using the -ignore flag.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class TfIdfNoveltyReducer
    extends MapReduceBase implements Reducer<
    HashBandWritable, DocumentWithVectorWritable, Text, VectorWritable> {

  private static final Logger logger =
      Logger.getLogger(TfIdfNoveltyReducer.class);

  private HashSet<Integer> parsedDocIds;
  private HashMap<Integer, Integer> docDates;
  private boolean ignoreDocs;

  @Override
  public void configure(JobConf config) {
    parsedDocIds = new HashSet<>();

    try {
      Path datesPath =
          new Path(config.get(Fields.DOC_DATES.get()));
      logger.info("Loading doc dates: " + datesPath);

      FileSystem fs = FileSystem.get(config);
      final SequenceFileProcessor datesReader = new IntegerSequenceFileReader(
          datesPath, fs, config);
      docDates = datesReader.processFile();
    } catch (IOException e) {
      logger.fatal("Error loading doc dates!", e);
    }
    logger.info("Loaded " + docDates.size() + " doc dates.");

    ignoreDocs = config.getBoolean(Fields.IGNORE.get(), false);
  }

  @Override
  public void reduce(HashBandWritable key,
      Iterator<DocumentWithVectorWritable> docBucket,
      OutputCollector<Text, VectorWritable> output, Reporter reporter)
      throws IOException {
    // TODO(uvictor): consider different strategies for selecting the NN:
    // closest date, squared nearest, first, random etc.
    final NearestNeighbourCollection<DocumentWithVector> docs =
        new SquaredNearestNeighbour(docDates);

    addDocBucket(docBucket, docs);

    if (docs.size() <= 1) {
      // No near neighbours.
      reporter
          .getCounter(TfIdfNovelty.Records.POTENTIALLY_IGNORED).increment(1);
      if (!ignoreDocs) {
        DocumentWithVector first = docs.iterator().next();
        output.collect(new Text(Integer.toString(first.getId())),
            new VectorWritable(first.getVector()));
      }
      return;
    }

    // doc id, nearest doc id
    final TreeMap<Integer, Integer> near = new TreeMap<>();
    for (DocumentWithVector current : docs) {
      final DocumentWithVector before = docs.getNearestNeighbour(current);
      if (before == null) {
        if (!ignoreDocs) {
          output.collect(new Text(Integer.toString(current.getId())),
              new VectorWritable(current.getVector()));
        }
        near.put(current.getId(), -1);
        continue;
      }
      near.put(current.getId(), before.getId());

      current
          .getVector().assign(before.getVector(), new DoubleDoubleFunction() {

        @Override
        public double apply(double arg, double other) {
          final double result = arg - other;
          if (result > 0) {
            return result;
          }
          return 0;
        }
      });

      output.collect(new Text(Integer.toString(current.getId())),
          new VectorWritable(current.getVector()));
    }
  }

  private void addDocBucket(Iterator<DocumentWithVectorWritable> docBucket,
      Collection<DocumentWithVector> docs) {
    while (docBucket.hasNext()) {
      final DocumentWithVectorWritable doc = docBucket.next();
      final int docId = Integer.parseInt(doc.getId().toString());
      docs.add(new DocumentWithVector(docId, doc.getVector().get()));
    }
    logger.info("Bucket size = " + docs.size());
  }
}
