package ch.ethz.las.wikimining.functions;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;

/**
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class WordCoverageSlow extends WordCoverage {

  private final Terms allTerms;

  // Might be costly to initialize, so we keep a copy here. Used in
  // {@link computeWordCoverage}.
  private TermsEnum termsEnum;
  private DocsEnum docsEnum;

  public WordCoverageSlow(IndexReader theReader, String theFieldName) throws IOException {
    super(theReader, theFieldName);

    allTerms = SlowCompositeReaderWrapper.wrap(theReader).terms(theFieldName);
  }

  /**
   * Computes the word coverage as equation (1) from the paper.
   *
   * Does this by iterating through all documents of a given term, for all
   * terms, which is slow.
   *
   * @param docsIds the set of documents for which to compute the word
   * coverage score.
   * @return the word coverage score
   *
   * @throws java.io.IOException
   */
  @Override
  public double compute(Set<Integer> docsIds) {
    assert docsIds != null;
    if (docsIds.isEmpty()) {
      return 0;
    }

    double sum = 0;
    try {
      termsEnum = allTerms.iterator(termsEnum);
      while (termsEnum.next() != null) {
        double maxTfIdf = 0;
        final Term term = new Term(fieldName, termsEnum.term());
        docsEnum = termsEnum.docs(null, docsEnum);

        while(docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          if (docsIds.contains(docsEnum.docID())) {
            // TODO(uvictor): try to memoize the maxima (for each word?)
            final double tfIdf = computeTfIdf(docsEnum.freq(), term);
            if (tfIdf > maxTfIdf) {
              maxTfIdf = tfIdf;
            }
          }
        }

        sum += computeWordWeight(term) * maxTfIdf;
      }
    } catch (IOException ex) {
      logger.warn("Could not read the Lucene index.");
    }

    return sum;
  }
}
