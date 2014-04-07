package ch.ethz.las.wikimining.mr.influence;

import ch.ethz.las.wikimining.functions.DocumentInfluence;
import ch.ethz.las.wikimining.base.Defaults;
import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.mr.io.IntegerSequenceFileReader;
import ch.ethz.las.wikimining.mr.io.VectorSequenceFileReader;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;

/**
 * Applies the SFO greedy algorithm for influential documents (eq 5) as a Reduce
 * stage, part of the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GreeDiReducer extends Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  private static final Logger logger = Logger.getLogger(GreeDiReducer.class);

  private HashMap<Integer, Integer> docDates;
  private HashMap<Integer, Vector> wordSpread;

  @Override
  public void setup(Context context) {
    try {
      final FileSystem fs = FileSystem.get(context.getConfiguration());

      final Path datesPath =
          new Path(context.getConfiguration().get(Fields.DOC_DATES.get()));
      final IntegerSequenceFileReader datesReader =
          new IntegerSequenceFileReader(
              datesPath, fs, context.getConfiguration());
      docDates = datesReader.read();

      final Path wordSpreadPath =
          new Path(context.getConfiguration().get(Fields.WORD_SPREAD.get()));
      final VectorSequenceFileReader wordSpreadReader =
          new VectorSequenceFileReader(
              wordSpreadPath, fs, context.getConfiguration());
      wordSpread = wordSpreadReader.read();
    } catch (IOException e) {
      logger.fatal("Error loading doc dates or word spread!", e);
    }
  }

  @Override
  public void reduce(IntWritable key,
      Iterable<DocumentWithVectorWritable> values, Context context)
      throws IOException, InterruptedException {
    final DocumentInfluence objectiveFunction =
        new DocumentInfluence(values, docDates, wordSpread);
    final SfoGreedyAlgorithm sfo = new SfoGreedyLazy(objectiveFunction);
    final int selectCount = context.getConfiguration()
        .getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());

    Set<Integer> selected =
        sfo.run(objectiveFunction.getAllDocIds(), selectCount);

    for (Integer docId : selected) {
      IntWritable outValue = new IntWritable(docId);
      context.write(NullWritable.get(), outValue);
    }
  }
}
