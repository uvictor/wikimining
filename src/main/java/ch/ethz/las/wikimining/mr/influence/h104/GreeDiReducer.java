package ch.ethz.las.wikimining.mr.influence.h104;

import ch.ethz.las.wikimining.functions.DocumentInfluence;
import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.mr.io.h104.IntegerSequenceFileReader;
import ch.ethz.las.wikimining.mr.io.h104.VectorSequenceFileReader;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;

/**
 * Applies the SFO greedy algorithm for influential documents (eq 5) as a Reduce
 * stage, part of the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GreeDiReducer extends MapReduceBase implements Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  private static final Logger logger = Logger.getLogger(GreeDiReducer.class);

  private HashMap<Integer, Integer> docDates;
  private HashMap<Integer, Vector> wordSpread;
  private int selectCount;

  @Override
  public void configure(JobConf config) {
    try {
      final FileSystem fs = FileSystem.get(config);

      final Path datesPath =
          new Path(config.get(Fields.DOC_DATES.get()));
      final IntegerSequenceFileReader datesReader =
          new IntegerSequenceFileReader(datesPath, fs, config);
      docDates = datesReader.processFile();

      final Path wordSpreadPath =
          new Path(config.get(Fields.WORD_SPREAD.get()));
      final VectorSequenceFileReader wordSpreadReader =
          new VectorSequenceFileReader(wordSpreadPath, fs, config);
      wordSpread = wordSpreadReader.processFile();
    } catch (IOException e) {
      logger.fatal("Error loading doc dates or word spread!", e);
    }

    selectCount =
        config.getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());
  }

  @Override
  public void reduce(IntWritable key,
      Iterator<DocumentWithVectorWritable> values,
      OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
      throws IOException {
    final DocumentInfluence objectiveFunction =
        new DocumentInfluence(values, docDates, wordSpread);
    final SfoGreedyAlgorithm sfo = new SfoGreedyLazy(objectiveFunction);


    Set<Integer> selected =
        sfo.run(objectiveFunction.getAllDocIds(), selectCount);

    for (Integer docId : selected) {
      IntWritable outValue = new IntWritable(docId);
      output.collect(NullWritable.get(), outValue);
    }
  }
}
