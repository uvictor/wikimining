package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.evaluate.WikiDatabase;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author uvictor
 */
public class WordCoverageTest {
  private static WikiDatabase wiki;
  private IndexReader reader;
  private WordCoverageFromLucene wordCoverage;

  @BeforeClass
  public static void setUpClass() throws Exception {
    wiki = new WikiDatabase();
    wiki.initialiseAndIndexForTest();
  }

  @AfterClass
  public static void tearDownClass() {
    wiki = null;
  }

  @Before
  public void setUp() throws Exception {
    reader = DirectoryReader.open(wiki.getIndexDir());
    wordCoverage =
        new WordCoverageFromLucene(reader, WikiDatabase.FieldNames.TEXT.toString());
  }

  @After
  public void tearDown() {
    wordCoverage = null;
    reader = null;
  }

  //@Test
  public void testComputeWordCoverageSingleDocOne() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(0);

    final double result = wordCoverage.compute(docsIds);
    Assert.assertTrue(result > 0);
  }

  //@Test
  public void testComputeWordCoverageSingleDocTwo() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(1);

    final double result = wordCoverage.compute(docsIds);
    Assert.assertTrue(result > 0);
  }

  //@Test
  public void testComputeWordCoverageConsecutiveSingleDocs() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(0);

    final double result = wordCoverage.compute(docsIds);
    Assert.assertTrue(result > 0);

    docsIds.remove(0);
    docsIds.add(1);
    final double resultTwo = wordCoverage.compute(docsIds);
    Assert.assertTrue(resultTwo > 0);
  }

  //@Test
  public void testComputeWordCoverageMultipleDocs() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(1);
    docsIds.add(4);
    docsIds.add(10);

    final double result = wordCoverage.compute(docsIds);
    Assert.assertTrue(result > 0);
  }

  //@Test
  public void testComputeWordCoverageVersusSlowSingleDoc() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(1);

    final WordCoverageFromLuceneSlow wordCoverageSlow =
        new WordCoverageFromLuceneSlow(reader, WikiDatabase.FieldNames.TEXT.toString());
    final double result = wordCoverage.compute(docsIds);
    final double resultSlow = wordCoverageSlow.compute(docsIds);
    assertEquals(resultSlow, result, 0.001);
  }

  @Test
  public void testComputeWordCoverageVersusSlowMultipleDocs() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(1);
    docsIds.add(4);
    docsIds.add(10);

    final WordCoverageFromLuceneSlow wordCoverageSlow =
        new WordCoverageFromLuceneSlow(reader, WikiDatabase.FieldNames.TEXT.toString());
    final double resultSlow = wordCoverageSlow.compute(docsIds);
    final double result = wordCoverage.compute(docsIds);
    assertEquals(result, resultSlow, 0.001);
  }
}
