
package ch.ethz.las.wikimining;

import de.tudarmstadt.ukp.wikipedia.api.Category;
import de.tudarmstadt.ukp.wikipedia.api.DatabaseConfiguration;
import de.tudarmstadt.ukp.wikipedia.api.Page;
import de.tudarmstadt.ukp.wikipedia.api.WikiConstants;
import de.tudarmstadt.ukp.wikipedia.api.Wikipedia;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiApiException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiInitializationException;
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiTitleParsingException;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

/**
 * TODO(uvictor): make sure to clean up as much unused resources as possible.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
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

  private static class IndexArticleCallable implements Callable<Boolean> {

    private final Logger logger;
    private final IndexWriter writer;
    private final Page article;

    public IndexArticleCallable(IndexWriter theWriter, Page theArticle) {
      logger = Logger.getLogger(this.getClass());
      writer = theWriter;
      article = theArticle;
    }

    @Override
    public Boolean call(){
      return indexArticle(writer, article);
    }

    private boolean indexArticle(IndexWriter writer, Page article) {
      String articleTitle = null;
      try {
        articleTitle = article.getTitle().getPlainTitle();
      } catch (WikiTitleParsingException ex) {
        logger.info("Article not logged.");
      }

      try {
        indexPage(writer, articleTitle, article.getPlainText());
      } catch (WikiApiException ex) {
        if (articleTitle != null) {
          logger.warn("Article not indexed: " + articleTitle);
        } else {
          logger.warn("Article not indexed.");
        }
        return false;
      }

      return true;
    }

    private void indexPage(IndexWriter writer, String title, String plainText) {
      final Document document = new Document();

      // StringField doesn't tokenize
      document.add(new StringField(
          ImportWiki.FieldNames.TITLE.toString(), title, Field.Store.YES));
      // Field which tokenizes and has the inverted indexes.
      FieldType indexedType = new FieldType();
      indexedType.setStored(false);
      indexedType.setTokenized(true);
      indexedType.setIndexed(true);
      indexedType.setStoreTermVectors(true);
      document.add(new Field(
          ImportWiki.FieldNames.TEXT.toString(), plainText,indexedType));
      try {
        writer.addDocument(document);
      } catch (IOException ex) {
        logger.warn("Page not indexed: " + title);
      }
    }
  }

  private final Logger logger;
  private final ExecutorService threadPool;
  private Wikipedia wiki;
  private Directory indexDir;
  private IndexWriterConfig indexConfig;
  private boolean initialized;

  public ImportWiki() {
    logger = Logger.getLogger(this.getClass());
    threadPool = Executors.newFixedThreadPool(WikiMining.THREAD_COUNT);
    initialized = false;
  }

  public void initialiseForReading()
      throws WikiInitializationException, IOException, WikiApiException {
    initialiseWikiDatabase();
    initializeLuceneOnHdd(new File("allIndex"));

    initialized = true;
  }

  public void initialiseAndIndex()
      throws WikiInitializationException, IOException, WikiApiException {
    initialiseWikiDatabase();
    initializeLuceneOnHdd(new File("testIndex"));
    indexWiki();

    initialized = true;
  }

  /**
   * Visible for tests.
   */
  public void initialiseForTest()
      throws WikiInitializationException, IOException, WikiApiException {
    initialiseWikiDatabase();
    initializeLuceneInRam();
    indexWikiForTest();

    initialized = true;
  }

  public void close() throws IOException {
    initialized = false;

    indexDir.close();
  }

  public Wikipedia getWiki() {
    assert initialized;
    return wiki;
  }

  public Directory getIndexDir() {
    assert initialized;
    return indexDir;
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

  private void initializeLuceneInRam() {
    final StandardAnalyzer analyzer
        = new StandardAnalyzer(Version.LUCENE_45);
    indexDir = new RAMDirectory();
    indexConfig = new IndexWriterConfig(Version.LUCENE_45, analyzer);
  }

  private void initializeLuceneOnHdd(File indexPath) throws IOException {
    final StandardAnalyzer analyzer
        = new StandardAnalyzer(Version.LUCENE_45);
    indexDir = new NIOFSDirectory(indexPath);
    indexConfig = new IndexWriterConfig(Version.LUCENE_45, analyzer);
  }

  private void indexWiki() throws IOException, WikiApiException {
    try (final IndexWriter writer = new IndexWriter(indexDir, indexConfig)) {
      indexAllWiki(writer);
//      indexCategoryRecursive(writer, wiki.getCategory("Mechanics"));
//      indexCategory(writer, wiki.getCategory("Windows games"));
//      indexCategory(writer, wiki.getCategory("Ball games"));
//      indexCategory(writer, wiki.getCategory("NEC_PC-9801_games"));
//      indexCategory(writer, wiki.getCategory("Video game genres"));
    }
  }

  private void indexWikiForTest() throws IOException, WikiApiException {
    try (final IndexWriter writer = new IndexWriter(indexDir, indexConfig)) {
      indexCategory(writer, wiki.getCategory("Video game genres"));
    }
  }

  private Category getMaxCategory(IndexWriter writer) {
    long maxPages = 0;
    Category maxCategory = null;

    for (Category category : wiki.getCategories()) {
      final long pages;
      try {
        pages = category.getNumberOfPages();
      } catch (WikiApiException ex) {
        logger.info("Category not considered for max.");
        continue;
      }

      if (pages > maxPages) {
        maxPages = pages;
        maxCategory = category;
      }
    }

    return maxCategory;
  }

  // TODO(uvictor): wait for completion of the future and be sure not to close
  // the writer before all the threads have finished their job; maybe use
  // writer.commit()?
  private int indexAllWiki(IndexWriter writer) {
    assert !(indexDir instanceof RAMDirectory);

    int indexedCount = 0;
    for (final Page article : wiki.getArticles()) {
      Future<Boolean> future =
          threadPool.submit(new IndexArticleCallable(writer, article));
      // TODO(uvictor): see how we can still print the count
      /*if (indexArticle(writer, article)) {
        indexedCount++;
        if (indexedCount % 1000 == 0) {
          logger.info("Indexed pages: " + indexedCount);
        }
      }*/
    }

    return indexedCount;
  }

  private void indexCategoryRecursive(IndexWriter writer, Category category) {
    indexCategory(writer, category);

    Iterable<Category> descendants = category.getDescendants();
    int count = 0;
    int pages = 0;
    for (Category subcategory : descendants) {
      pages += indexCategory(writer, subcategory);
      count++;
      logger.info("Indexed " + count + " categories with " + pages + " pages.");
    }
  }

  private void indexCategoryWithChildren(
      IndexWriter writer, Category category) {
    indexCategory(writer, category);

    Set<Category> children = category.getChildren();
    int count = 0;
    int pages = 0;
    for (Category subcategory : children) {
      pages += indexCategory(writer, subcategory);
      count++;
      logger.info("Indexed " + count + " categories with " + pages + " pages.");
    }
  }

  private int indexCategory(IndexWriter writer, Category category) {
    String categoryTitle = null;
    try {
      categoryTitle = category.getTitle().getPlainTitle();
      logger.info("Category " + categoryTitle + " with "
          + category.getNumberOfPages() + " pages.");
    } catch (WikiApiException ex) {
      logger.info("Category not logged.");
    }

    final Set<Page> articles;
    try {
      articles = category.getArticles();
    } catch (WikiApiException ex) {
      if (categoryTitle != null) {
        logger.warn("Category not indexed: " + categoryTitle);
      }
      return -1;
    }

    int indexedCount = 0;
    for (final Page article : articles) {
      new IndexArticleCallable(writer, article).call();
    }

    return indexedCount;
  }
}
