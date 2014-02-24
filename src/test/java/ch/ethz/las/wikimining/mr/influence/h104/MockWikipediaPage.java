
package ch.ethz.las.wikimining.mr.influence.h104;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class MockWikipediaPage extends WikipediaPage {
  private String mockId;
  private boolean mockArticle;
  private boolean mockDisambig;
  private boolean mockRedirect;
  private boolean mockStub;

  public MockWikipediaPage() {
    super();
  }

  public MockWikipediaPage(String theMockId, boolean theMockArticle,
      boolean theMockDisambig, boolean theMockRedirect, boolean theMockStub) {
    this();

    mockId = theMockId;
    mockArticle = theMockArticle;
    mockDisambig = theMockDisambig;
    mockRedirect = theMockRedirect;
    mockStub = theMockStub;
  }

  @Override
  protected void processPage(String content) {
    this.language = "en";
    this.title = content;
    this.textStart = 0;
    this.textEnd = 3;

    this.mId = mockId;
    this.isArticle = mockArticle;
    this.isDisambig = mockDisambig;
    this.isRedirect = mockRedirect;
    this.isStub = mockStub;
  }
}
