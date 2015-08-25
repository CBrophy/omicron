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

import com.google.common.collect.ComparisonChain;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class LogEntry implements Comparable<LogEntry> {
  private static final AtomicLong ENTRY_IDS = new AtomicLong();

  private final long timestamp;
  private final long entryId = ENTRY_IDS.incrementAndGet();

  public LogEntry() {
    this.timestamp = DateTime.now().getMillis();
  }

  public LogEntry(final long timestamp) {
    checkArgument(timestamp > 0, "timestamp must be positive");

    this.timestamp = timestamp;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public int compareTo(LogEntry logEntry) {
    checkNotNull(logEntry, "logEntry");

    return ComparisonChain
      .start()
      .compare(timestamp, logEntry.timestamp)
      .compare(entryId, logEntry.entryId)
      .result();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof LogEntry && entryId == ((LogEntry) obj).entryId;
  }

  public long getEntryId() {
    return entryId;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
