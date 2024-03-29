
package ch.ethz.las.wikimining;

import ch.ethz.las.wikimining.evaluate.WikiDatabase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class ImportWikiTest {

  private WikiDatabase wiki;

  @Before
  public void setUp() {
    wiki = new WikiDatabase();
  }

  @After
  public void tearDown() {
    wiki = null;
  }

  @Test
  public void testInitialise() throws Exception {
    wiki.initialiseAndIndexForTest();
    try (final IndexReader reader = DirectoryReader.open(wiki.getIndexDir())) {
      assertEquals(72, reader.numDocs());
      assertEquals(72, reader.maxDoc());
    }
  }
}
