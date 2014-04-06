
package ch.ethz.las.wikimining.base;

import java.util.Collection;

/**
 * Decorates a collection with a nearest neighbour retrieval method.
 *
 * @author Victor Ungureanu (uvictor@student.ethz.ch)
 */
public interface NearestNeighbourCollection<E> extends Collection<E> {
  public abstract E getNearestNeighbour(E current);
}
