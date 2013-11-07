
package wikimining;

import de.tudarmstadt.ukp.wikipedia.api.Category;
import de.tudarmstadt.ukp.wikipedia.api.DatabaseConfiguration;
import de.tudarmstadt.ukp.wikipedia.api.Page;
import de.tudarmstadt.ukp.wikipedia.api.WikiConstants;
import de.tudarmstadt.ukp.wikipedia.api.Wikipedia;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import java.io.IOException;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class ImportWiki {

  public static enum FieldNames {

    TITLE("title"),
    TEXT("text");

    private final String name;

    private FieldNames(String theName) {
      this.name = theName;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private Wikipedia wiki;
  private Directory indexDir;
  private IndexWriterConfig indexConfig;
  private boolean initialized;

  public ImportWiki() {
    initialized = false;
  }

  public void initialise()
      throws WikiInitializationException, IOException, WikiApiException {
    initialiseWikiDatabase();
    initializeLucene();
    indexWiki();

    initialized = true;
  }

  public Wikipedia getWiki() {
    return wiki;
  }

  public Directory getIndexDir() {
    assert initialized;
    return indexDir;
  }

  /**
   * Visible for tests.
   */
  void initialiseForTest()
      throws WikiInitializationException, IOException, WikiApiException {
    initialiseWikiDatabase();
    initializeLucene();
    indexWikiForTest();

    initialized = true;
  }

  private void initialiseWikiDatabase() throws WikiInitializationException {
    // Configure the database connection parameters
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration();
    dbConfig.setHost("localhost");
    dbConfig.setDatabase("wiki");
    dbConfig.setUser("root");
    dbConfig.setPassword("thisisnosecret");
    dbConfig.setLanguage(WikiConstants.Language.english);

    // Create the Wikipedia object
    wiki = new Wikipedia(dbConfig);
  }

  private void initializeLucene() {
    final StandardAnalyzer analyzer
        = new StandardAnalyzer(Version.LUCENE_45);
    indexDir = new RAMDirectory();
    indexConfig = new IndexWriterConfig(Version.LUCENE_45, analyzer);
  }

  private void indexWiki() throws IOException, WikiApiException {
    try (final IndexWriter writer = new IndexWriter(indexDir, indexConfig)) {
      indexCategory(writer, wiki.getCategory("Ball games"));
      indexCategory(writer, wiki.getCategory("NEC_PC-9801_games"));
      indexCategory(writer, wiki.getCategory("Video game genres"));
    }
  }

  private void indexCategory(IndexWriter writer, Category category)
      throws IOException, WikiApiException {
    System.out.println("Category " + category.getTitle() + " with "
        + category.getNumberOfPages() + " pages.");

    final Set<Page> articles = category.getArticles();
    for (final Page article : articles) {
      indexPage(
          writer, article.getTitle().getPlainTitle(), article.getPlainText());
    }
    System.out.println("It really has " + articles.size() + " pages.");
  }

  private Category getMaxCategory(IndexWriter writer) throws WikiApiException {
    long maxPages = 0;
    Category maxCategory = null;
    for (Category category : wiki.getCategories()) {
      final long pages = category.getNumberOfPages();
      if (pages > maxPages) {
        maxPages = pages;
        maxCategory = category;
      }
    }

    return maxCategory;
  }

  private void indexWikiForTest() throws IOException, WikiApiException {
    try (final IndexWriter writer = new IndexWriter(indexDir, indexConfig)) {
      final Category category = wiki.getCategory("Video game genres");
      final Set<Page> articles = category.getArticles();
      for (final Page article : articles) {
        indexPage(
            writer, article.getTitle().getPlainTitle(), article.getPlainText());
      }
    }
  }

  private void indexPage(IndexWriter writer, String title, String plainText)
      throws IOException {
    final Document document = new Document();

    // StringField doesn't tokenize
    document.add(
        new StringField(FieldNames.TITLE.toString(), title, Field.Store.YES));
    // Field which tokenizes and has the inverted indexes.
    FieldType indexedType = new FieldType();
    indexedType.setStored(false);
    indexedType.setTokenized(true);
    indexedType.setIndexed(true);
    indexedType.setStoreTermVectors(true);
    document.add(new Field(FieldNames.TEXT.toString(), plainText,indexedType));

    writer.addDocument(document);
  }
}
