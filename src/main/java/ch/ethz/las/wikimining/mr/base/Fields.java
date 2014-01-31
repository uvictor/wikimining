package ch.ethz.las.wikimining.mr.base;

/**
 * List of field names for MapReduce.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public enum Fields {

  BANDS("bands"),
  COMPRESSION("compression"),
  DIMENSIONS("dimensions"),
  DOC_DATES("docDates"),
  DOCS_SUBSET("docSubset"),
  INPUT("input"),
  LANGUAGE("language"),
  OUTPUT("output"),
  PARTITION_COUNT("partitionCount"),
  ROWS("rows"),
  SELECT_COUNT("selectCount"),
  WORD_SPREAD("wordSpread"),
  ;

  private final String value;

  private Fields(String theValue) {
    value = theValue;
  }

  public String get() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }
}
