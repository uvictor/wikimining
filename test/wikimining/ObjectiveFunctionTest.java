
package wikimining;

import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import java.io.IOException;
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

public class ObjectiveFunctionTest {

  private static ImportWiki wiki;
  private ObjectiveFunction function;

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
    function =
        new ObjectiveFunction(reader, ImportWiki.FieldNames.TEXT.toString());
  }

  @After
  public void tearDown() {
    function = null;
  }

  @Test
  public void testComputeWordCoverageSingleDocOne() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(0);

    final double result = function.computeWordCoverage(docsIds);
    Assert.assertTrue(result > 0);
  }

  @Test
  public void testComputeWordCoverageSingleDocTwo() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(1);

    final double result = function.computeWordCoverage(docsIds);
    Assert.assertTrue(result > 0);
  }

  @Test
  public void testComputeWordCoverageConsecutiveSingleDocs() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(0);

    final double result = function.computeWordCoverage(docsIds);
    Assert.assertTrue(result > 0);

    docsIds.remove(0);
    docsIds.add(1);
    final double resultTwo = function.computeWordCoverage(docsIds);
    Assert.assertTrue(resultTwo > 0);
  }

  @Test
  public void testComputeWordCoverageMultipleDocs() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(1);
    docsIds.add(4);
    docsIds.add(10);

    final double result = function.computeWordCoverage(docsIds);
    Assert.assertTrue(result > 0);
  }

    @Test
  public void testComputeWordCoverageVersusSlow() throws Exception {
    final Set<Integer> docsIds = new HashSet<>();
    docsIds.add(1);
    docsIds.add(4);
    docsIds.add(10);

    function.initializeSlowComputations();
    final double result = function.computeWordCoverage(docsIds);
    final double resultSlow = function.computeWordCoverageSlow(docsIds);
    assertEquals(resultSlow, result, 1e-5);
  }
}
