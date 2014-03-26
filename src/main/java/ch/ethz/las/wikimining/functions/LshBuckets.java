
package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.base.HashBandWritable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Computes the LSH-buckets submodular function.
 *
 * Eg. F(S) = sum_i^#b{ concave(|S n B_i|) * |B_i| }
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class LshBuckets implements ObjectiveFunction {

  private final HashMap<HashBandWritable, HashSet<Integer>> buckets;

  /**
   * Creates an object used to compute the necessary word coverage score.
   */
  public LshBuckets(HashMap<HashBandWritable, HashSet<Integer>> theBuckets) {
    buckets = theBuckets;
  }

  /**
   * F(S) = sum_i^#b{ g(|S n B_i|) / g(|Bi|) * |B_i| } / sum_i^#b |Bi|
   * g concave (eg sqrt).
   */
  @Override
  public double compute(Set<Integer> docIds) {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    double sum = 0;
    double totalSize = 0;
    for (HashSet<Integer> bucket : buckets.values()) {
      sum += concaveFunction(intersect(docIds, bucket)) * (double) bucket.size()
          / concaveFunction(bucket.size());
      totalSize += bucket.size();
    }

    return sum / totalSize;
  }

  public double intersect(Set<Integer> docIds, Set<Integer> bucket) {
    int count = 0;
    for (Integer docId : docIds) {
      if (bucket.contains(docId)) {
        count++;
      }
    }

    return count;
  }

  public double concaveFunction(double value) {
    return Math.sqrt(value);
  }
}
