package com.zulily.omicron;

import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;


public class EvictingTreeSet<E> extends TreeSet<E> {

  private final int sizeLimit;
  private final boolean evictFirst;

  public EvictingTreeSet(final int sizeLimit, final boolean evictFirst) {

    super();

    checkArgument(sizeLimit > 0, "sizeLimit must be a positive integer");
    this.sizeLimit = sizeLimit;
    this.evictFirst = evictFirst;
  }

  public EvictingTreeSet(final int sizeLimit, final boolean evictFirst, final Comparator<? super E> comparator) {

    super(comparator);

    checkArgument(sizeLimit > 0, "sizeLimit must be a positive integer");
    this.sizeLimit = sizeLimit;
    this.evictFirst = evictFirst;
  }

  public EvictingTreeSet(final int sizeLimit, final boolean evictFirst, final Collection<? extends E> c) {

    super(c);

    checkArgument(sizeLimit > 0, "sizeLimit must be a positive integer");
    this.sizeLimit = sizeLimit;
    this.evictFirst = evictFirst;
    this.evict();
  }

  public EvictingTreeSet(final int sizeLimit, final boolean evictFirst, final SortedSet<E> s) {
    super(s);

    checkArgument(sizeLimit > 0, "sizeLimit must be a positive integer");
    this.sizeLimit = sizeLimit;
    this.evictFirst = evictFirst;
    this.evict();
  }

  @Override
  public boolean add(E e) {

    boolean result = super.add(e);
    this.evict();
    return result;


  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    boolean result = super.addAll(c);
    this.evict();
    return result;

  }

  @Override
  public Object clone() {
    final EvictingTreeSet<E> clone = new EvictingTreeSet<>(this.sizeLimit, this.evictFirst, comparator());

    clone.addAll(this);

    return clone;

  }


  public int getSizeLimit() {
    return sizeLimit;
  }

  private void evict() {
    while (size() > sizeLimit) {
      remove(evictFirst ? first() : last());
    }
  }
}
