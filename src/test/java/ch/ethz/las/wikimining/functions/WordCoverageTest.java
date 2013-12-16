/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.ethz.las.wikimining.functions;

import ch.ethz.las.wikimining.ImportWiki;
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
  private static ImportWiki wiki;
  private IndexReader reader;
  private WordCoverage wordCoverage;

  @BeforeClass
  public static void setUpClass() throws Exception {
    wiki = new ImportWiki();
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
        new WordCoverage(reader, ImportWiki.FieldNames.TEXT.toString());
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

    final WordCoverageSlow wordCoverageSlow =
        new WordCoverageSlow(reader, ImportWiki.FieldNames.TEXT.toString());
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

    final WordCoverageSlow wordCoverageSlow =
        new WordCoverageSlow(reader, ImportWiki.FieldNames.TEXT.toString());
    final double resultSlow = wordCoverageSlow.compute(docsIds);
    final double result = wordCoverage.compute(docsIds);
    assertEquals(result, resultSlow, 0.001);
  }
}
