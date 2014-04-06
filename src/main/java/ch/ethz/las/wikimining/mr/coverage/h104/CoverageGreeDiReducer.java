
package ch.ethz.las.wikimining.mr.coverage.h104;

import ch.ethz.las.wikimining.functions.WeightedWordCoverage;
import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.base.Defaults;
import ch.ethz.las.wikimining.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.base.Fields;
import ch.ethz.las.wikimining.mr.utils.h104.FakeIntLongSequenceFileReader;
import ch.ethz.las.wikimining.mr.utils.h104.IntLongSequenceFileReader;
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

/**
 * Applies the SFO greedy algorithm for word coverage (eq 1) from a Mahout
 * index as a Reduce stage, part of the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class CoverageGreeDiReducer extends MapReduceBase implements Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  private static final Logger logger = Logger.getLogger(CoverageGreeDiReducer.class);

  private HashMap<Integer, Long> wordCount;
  private int selectCount;

  @Override
  public void configure(JobConf config) {
    wordCount = readWordCount(config);
    selectCount =
        config.getInt(Fields.SELECT_COUNT.get(), Defaults.SELECT_COUNT.get());
  }

  @Override
  public void reduce(IntWritable key,
      Iterator<DocumentWithVectorWritable> values,
      OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
      throws IOException {
    final WordCoverageFromMahout objectiveFunction;
    if (wordCount == null) {
      objectiveFunction = new WordCoverageFromMahout(values);
    } else {
      objectiveFunction = new WeightedWordCoverage(wordCount, values);
    }
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

  public static HashMap<Integer, Long> readWordCount(JobConf config) {
    final String wordCountPath = config.get(Fields.WORD_COUNT.get());
    if (wordCountPath == null) {
      return null;
    }

    try {
      final FileSystem fs = FileSystem.get(config);
      final Path wordCountsPath = new Path(wordCountPath);

      final IntLongSequenceFileReader bucketsReader;
      if ("wc".equals(config.get(Fields.WORD_COUNT_TYPE.get()))) {
        bucketsReader =
            new FakeIntLongSequenceFileReader(wordCountsPath, fs, config);
      } else {
        bucketsReader =
            new IntLongSequenceFileReader(wordCountsPath, fs, config);
      }

      return bucketsReader.processFile();
    } catch (IOException e) {
      logger.fatal("Error loading buckets!", e);
    }

    return null;
  }
}
