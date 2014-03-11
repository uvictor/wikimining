package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.evaluate.WikiDatabase;
import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import ch.ethz.las.wikimining.functions.WordCoverageFromLucene;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for comparing SfoGreedyLazy with SfoGreedyNonLazy.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyLazyTest {

  private static WikiDatabase wiki;
  private SfoGreedyAlgorithm greedyNonLazy;
  private SfoGreedyAlgorithm greedyLazy;
  private int docsCount;

  @BeforeClass
  public static void setUpClass() throws IOException, WikiApiException {
    wiki = new WikiDatabase();
    wiki.initialiseAndIndexForTest();
  }

  @AfterClass
  public static void tearDownClass() {
    wiki = null;
  }

  @Before
  public void setUp() throws IOException {
    final IndexReader reader = DirectoryReader.open(wiki.getIndexDir());
    final ObjectiveFunction objectiveFunction
        = new WordCoverageFromLucene(reader, WikiDatabase.FieldNames.TEXT.toString());

    docsCount = reader.numDocs();
    greedyNonLazy = new SfoGreedyNonLazy(objectiveFunction);
    greedyLazy = new SfoGreedyLazy(objectiveFunction);
  }

  @After
  public void tearDown() {
    greedyLazy = null;
  }

  @Test
  public void testRunAndRunNonLazilyOne() throws Exception {
    final Set<Integer> resultNonLazy = greedyNonLazy.run(docsCount, 1);
    final Set<Integer> result = greedyLazy.run(docsCount, 1);

    assertEquals(resultNonLazy, result);
  }

  @Test
  public void testRunAndRunNonLazilyMultiple() throws Exception {
    final Set<Integer> resultNonLazy = greedyNonLazy.run(docsCount, 3);
    final Set<Integer> result = greedyLazy.run(docsCount, 3);

    assertEquals(resultNonLazy, result);
  }
}
