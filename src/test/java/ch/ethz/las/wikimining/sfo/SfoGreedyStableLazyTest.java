package ch.ethz.las.wikimining.sfo;

import ch.ethz.las.wikimining.ImportWiki;
import ch.ethz.las.wikimining.functions.ObjectiveFunction;
import ch.ethz.las.wikimining.functions.WordCoverage;
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
 * Tests for comparing SfoGreedyStableLazy with SfoGreedyNonLazy.
 * <p>
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class SfoGreedyStableLazyTest {

  private static ImportWiki wiki;
  private SfoGreedyAlgorithm greedyNonLazy;
  private SfoGreedyAlgorithm greedyStableLazy;
  private int docsCount;

  @BeforeClass
  public static void setUpClass() throws IOException, WikiApiException {
    wiki = new ImportWiki();
    wiki.initialiseForTest();
  }

  @AfterClass
  public static void tearDownClass() {
    wiki = null;
  }

  @Before
  public void setUp() throws IOException {
    final IndexReader reader = DirectoryReader.open(wiki.getIndexDir());
    final ObjectiveFunction objectiveFunction
        = new WordCoverage(reader, ImportWiki.FieldNames.TEXT.toString());

    docsCount = reader.numDocs();
    greedyNonLazy = new SfoGreedyNonLazy(objectiveFunction);
    greedyStableLazy = new SfoGreedyStableLazy(objectiveFunction);
  }

  @After
  public void tearDown() {
    greedyStableLazy = null;
  }

  @Test
  public void testRunAndRunNonLazilyOne() throws Exception {
    final Set<Integer> resultNonLazy = greedyNonLazy.run(docsCount, 1);
    final Set<Integer> result = greedyStableLazy.run(docsCount, 1);

    assertEquals(resultNonLazy, result);
  }

  @Test
  public void testRunAndRunNonLazilyMultiple() throws Exception {
    final Set<Integer> resultNonLazy = greedyNonLazy.run(docsCount, 3);
    final Set<Integer> result = greedyStableLazy.run(docsCount, 3);

    assertEquals(resultNonLazy, result);
  }
}
