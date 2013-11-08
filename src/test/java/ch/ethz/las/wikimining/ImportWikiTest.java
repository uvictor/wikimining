
package ch.ethz.las.wikimining;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class ImportWikiTest {

  private ImportWiki wiki;

  @Before
  public void setUp() {
    wiki = new ImportWiki();
  }

  @After
  public void tearDown() {
    wiki = null;
  }

  @Test
  public void testInitialise() throws Exception {
    wiki.initialiseForTest();
    try (final IndexReader reader = DirectoryReader.open(wiki.getIndexDir())) {
      assertEquals(152, reader.numDocs());
      assertEquals(152, reader.maxDoc());
    }
  }
}
