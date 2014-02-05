package ch.ethz.las.wikimining.mr.base;

/**
 * List of field names for MapReduce.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public enum Fields {

  BANDS("bands"),
  BASIS("basis"),
  COMPRESSION("compression"),
  DIMENSIONS("dimensions"),
  DOC_DATES("dates"),
  DOCS_SUBSET("docs"),
  INPUT("input"),
  LANGUAGE("language"),
  OUTPUT("output"),
  PARTITION_COUNT("partitions"),
  ROWS("rows"),
  SELECT_COUNT("select"),
  WORD_SPREAD("spread"),
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
