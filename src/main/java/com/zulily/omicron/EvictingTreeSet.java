/*
 * Copyright (C) 2014 zulily, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zulily.omicron;

import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A sorted set that evicts items from either the top or bottom of the
 * sort when a specific set size is reached
 */
public class EvictingTreeSet<E> extends TreeSet<E> {

  private final int sizeLimit;
  private final boolean evictFirst;

  /**
   * Constructor
   *
   * @param sizeLimit  The inclusive limit after which items will be removed
   * @param evictFirst Set to true to evict the first item or false to evict the last instead
   */
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

  @SuppressWarnings("CloneDoesntCallSuperClone")
  @Override
  public Object clone() {
    final EvictingTreeSet<E> clone = new EvictingTreeSet<>(this.sizeLimit, this.evictFirst, comparator());

    clone.addAll(this);

    return clone;

  }

  /**
   * The size limit (inclusive) after which items will be evicted from the set
   *
   * @return The size limit
   */
  public int getSizeLimit() {
    return sizeLimit;
  }

  /**
   * Whether or not to evict items from the first or last part of the sort
   *
   * @return True if items are evicted from the first part, false to evict the last
   */
  public boolean isEvictFirst() {
    return evictFirst;
  }

  private void evict() {
    while (size() > sizeLimit) {
      remove(evictFirst ? first() : last());
    }
  }
}
