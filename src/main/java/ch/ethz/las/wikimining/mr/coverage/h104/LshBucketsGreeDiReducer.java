
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.functions.LshBuckets;
import ch.ethz.las.wikimining.functions.ObjectiveCombiner;
import ch.ethz.las.wikimining.functions.WeightedWordCoverage;
import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.base.Defaults;
import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.base.HashBandWritable;
import ch.ethz.las.wikimining.mr.io.h104.BucketsSequenceFileReader;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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

/**
 * Applies the SFO greedy algorithm for LSH buckets as a Reduce stage, part of
 * the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class LshBucketsGreeDiReducer extends MapReduceBase implements Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  private static final Logger logger =
      Logger.getLogger(LshBucketsGreeDiReducer.class);

  private HashMap<HashBandWritable, HashSet<Integer>> buckets;
  private HashMap<Integer, Long> wordCount;
  private int selectCount;

  @Override
  public void configure(JobConf config) {
    try {
      final FileSystem fs = FileSystem.get(config);
      final Path bucketsPath = new Path(config.get(Fields.BUCKETS.get()));
      final BucketsSequenceFileReader bucketsReader =
          new BucketsSequenceFileReader(bucketsPath, fs, config);
      buckets = bucketsReader.processFile();
    } catch (IOException e) {
      logger.fatal("Error loading buckets!", e);
    }

    wordCount = CoverageGreeDiReducer.readWordCount(config);
    selectCount =
        config.getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());
  }

  @Override
  public void reduce(IntWritable key,
      Iterator<DocumentWithVectorWritable> values,
      OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
      throws IOException {
    final WordCoverageFromMahout wordCoverage =
        new WeightedWordCoverage(wordCount, values);
    logger.info("Created WordCoverageFromMahout");
    final LshBuckets lshBuckets = new LshBuckets(buckets);
    logger.info("Created LshBuckets");
    final ObjectiveCombiner combiner = new ObjectiveCombiner(
        Arrays.asList(1D, 1D), wordCoverage, lshBuckets);

    final SfoGreedyAlgorithm sfo =
        new SfoGreedyLazy(combiner, reporter);
    logger.info("Created SfoGreedyAlgorithm");

    Set<Integer> selected =
        sfo.run(wordCoverage.getAllDocIds(), selectCount);
    logger.info("Finished running SFO");

    for (Integer docId : selected) {
      IntWritable outValue = new IntWritable(docId);
      output.collect(NullWritable.get(), outValue);
    }
  }
}
