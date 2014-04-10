/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.ethz.las.wikimining.mr.coverage;

import ch.ethz.las.wikimining.functions.WordCoverageFromMahout;
import ch.ethz.las.wikimining.mr.base.Defaults;
import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.Fields;
import ch.ethz.las.wikimining.sfo.SfoGreedyAlgorithm;
import ch.ethz.las.wikimining.sfo.SfoGreedyLazy;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Applies the SFO greedy algorithm for word coverage (eq 1) as a Reduce
 * stage, part of the GreeDi protocol.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class GreeDiReducer extends Reducer<
    IntWritable, DocumentWithVectorWritable, NullWritable, IntWritable> {

  @Override
  public void reduce(IntWritable key,
      Iterable<DocumentWithVectorWritable> values, Context context)
      throws IOException, InterruptedException {
    final WordCoverageFromMahout objectiveFunction =
        new WordCoverageFromMahout(values);
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
