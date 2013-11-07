package wikimining;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.util.BytesRef;

/**
 * Class that offers methods to compute the objective function for a submodular
 * function maximisation (SFO) algorithm.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public class ObjectiveFunction {

  private static class Score {
    double wordWeight;
    double maxTfIdf;

    public Score(double tf, double idf) {
      this.wordWeight = tf;
      this.maxTfIdf = idf;
    }

    public double getWordWeight() {
      return wordWeight;
    }

    public double getMaxTfIdf() {
      return maxTfIdf;
    }
  }

  private final IndexReader reader;
  private final String fieldName;
  private final DefaultSimilarity similarity;
  private Terms allTerms = null;

  // Might be costly to initialize, so we keep a copy here. Used in
  // {@link computeWordCoverage}.
  private TermsEnum termsEnum;
  private DocsEnum docsEnum;

  /**
   * Creates an object used to compute the necessary word coverage score.
   *
   * @param theReader used to get the documents' scores
   * @param theFieldName name of the indexed field
   *
   * @throws java.io.IOException
   */
  public ObjectiveFunction(IndexReader theReader, String theFieldName) {
    reader = theReader;
    fieldName = theFieldName;

    similarity = new DefaultSimilarity();
  }

  public void initializeSlowComputations() throws IOException {
    allTerms = SlowCompositeReaderWrapper.wrap(reader).terms(fieldName);
  }

  /**
   * Computes the word coverage as equation (1) from the paper.
   *
   * @param docIds the set of documents for which to compute the word
   * coverage score.
   * @return the word coverage score
   *
   * @throws java.io.IOException
   */
  public double computeWordCoverage(Set<Integer> docIds) throws IOException {
    assert docIds != null;
    if (docIds.isEmpty()) {
      return 0;
    }

    // Max tf-idfs for all terms from the docIds documents.
    final Map<BytesRef, Score> maxScores = new HashMap<>();
    for (Integer docId : docIds) {
      final Terms terms = reader.getTermVector(docId, fieldName);

      termsEnum = terms.iterator(termsEnum);
      while (termsEnum.next() != null) {
        final Term term = new Term(fieldName, termsEnum.term());
        docsEnum = termsEnum.docs(null, docsEnum);
        docsEnum.nextDoc();
        final double tfIdf = computeTfIdf(docsEnum.freq(), term);

        if (!maxScores.containsKey(termsEnum.term())) {
          maxScores
              .put(termsEnum.term(), new Score(computeWordWeight(term), tfIdf));
        } else {
          final Score score = maxScores.get(termsEnum.term());
          if (tfIdf > score.getMaxTfIdf()) {
            maxScores
                .put(termsEnum.term(), new Score(score.getWordWeight(), tfIdf));
          }
        }
      }
    }

    double sum = 0;
    for (Score score : maxScores.values()) {
      sum += score.getWordWeight() * score.getMaxTfIdf();
    }

    return sum;
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
  public double computeWordCoverageSlow(Set<Integer> docsIds)
      throws IOException {
    assert docsIds != null;
    if (docsIds.isEmpty()) {
      return 0;
    }

    double sum = 0;
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

    return sum;
  }

  /**
   * Returns the word weight as the tf score.
   *
   * @param term the word
   * @return the word weight
   */
  private double computeWordWeight(Term term) throws IOException {
    long totalTermFreq = reader.totalTermFreq(term);

    return similarity.tf(totalTermFreq);
  }

  /**
   * Returns the tf-idf score for a given (term frequency within a) document
   * and word.
   *
   * @param termFreq number of times the word appears in the current document
   * @param term the word
   * @return the tf-idf score
   */
  private double computeTfIdf(int termFreq, Term term) throws IOException {
    double tf = similarity.tf(termFreq);
    double idf = similarity.idf(reader.docFreq(term), reader.numDocs());

    return tf * idf;
  }
}
