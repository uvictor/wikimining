
package wikimining;

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

public class SfoGreedyLazyTest {
private static ImportWiki wiki;
  private SfoGreedyLazy sfo;

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
    sfo = new SfoGreedyLazy(reader, ImportWiki.FieldNames.TEXT.toString());
  }

  @After
  public void tearDown() {
    sfo = null;
  }

  @Test
  public void testRunAndRunNonLazilyOne() throws Exception {
    final Set<Integer> resultNonLazy = sfo.runNonLazily(1);
    final Set<Integer> result = sfo.run(1);

    assertEquals(resultNonLazy, result);
  }

  @Test
  public void testRunAndRunNonLazilyMultiple() throws Exception {
    final Set<Integer> resultNonLazy = sfo.runNonLazily(3);
    final Set<Integer> result = sfo.run(3);

    assertEquals(resultNonLazy, result);
  }
}
