
package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.mr.base.DocumentWithVectorWritable;
import ch.ethz.las.wikimining.mr.base.HashBandWritable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.random.RandomProjector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Compares results of different objective functions/combinations.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class ObjectiveCombinerTest {

  private static Random random;

  private ArrayList<DocumentWithVectorWritable> documents;
  private HashMap<HashBandWritable, HashSet<Integer>> buckets;
  private WordCoverageFromMahout wordCoverage;
  private LshBuckets lshBuckets;

  @BeforeClass
  public static void setUp() {
    random = new Random();
  }

  @AfterClass
  public static void tearDown() {
    random = null;
  }

  /**
   * Test of compute method, of class ObjectiveCombiner.
   */
  @Test
  public void testCompute() {
    createVectors(8, 16);
    createBuckets(8, 8);
    wordCoverage = new WordCoverageFromMahout(documents);
    lshBuckets = new LshBuckets(buckets);

    Set<Integer> subset = new HashSet<>(3);
    subset.addAll(Arrays.asList(2, 3, 7));

    System.out.println(
        wordCoverage.compute(subset) + " " + lshBuckets.compute(subset));
  }

  private void createVectors(int docCount, int dimensions) {
    documents = new ArrayList<>(docCount);
    final Matrix vectors =
        RandomProjector.generateBasisNormal(docCount, dimensions);

    for (int i = 0; i < docCount; i++) {
      final DocumentWithVectorWritable doc = new DocumentWithVectorWritable(
          new Text(new Integer(i).toString()),
          new VectorWritable(vectors.viewRow(i).normalize()));
      documents.add(doc);
    }
  }

  private void createBuckets(int bucketCount, int approxMaxBucketSize) {
    buckets = new HashMap<>(bucketCount);

    for (int i = 0; i < bucketCount; i++) {
      // bucketSize = 1 .. approxMaxBucketSize+1 whp
      final int bucketSize = (int)
          (Math.abs(random.nextGaussian() + 3) * approxMaxBucketSize / 6 + 1);
      final HashSet<Integer> bucket = new HashSet<>(bucketSize);

      for (int j = 0; j < bucketSize; j++) {
        bucket.add(random.nextInt(documents.size()));
      }

      buckets.put(new HashBandWritable(i, i), bucket);
    }
  }
}
