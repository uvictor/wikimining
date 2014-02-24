
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Applies the SFO greedy algorithm for word coverage (eq 1) from a Mahout
 * index as a Reduce stage, part of the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GreeDiReducer extends MapReduceBase implements Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  private static final Logger logger = Logger.getLogger(GreeDiReducer.class);

  private int selectCount;

  @Override
  public void configure(JobConf job) {
    selectCount =
        job.getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());
  }

  @Override
  public void reduce(IntWritable key,
      Iterator<DocumentWithVectorWritable> values,
      OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
      throws IOException {
    final WordCoverageFromMahout objectiveFunction =
        new WordCoverageFromMahout(values);
    logger.info("Created WordCoverageFromMahout");
    final SfoGreedyAlgorithm sfo =
        new SfoGreedyLazy(objectiveFunction, reporter);
    logger.info("Created SfoGreedyAlgorithm");
    Set<Integer> selected =
        sfo.run(objectiveFunction.getAllDocIds(), selectCount);
    logger.info("Finished running SFO");

    for (Integer docId : selected) {
      IntWritable outValue = new IntWritable(docId);
      output.collect(NullWritable.get(), outValue);
    }
  }
}
