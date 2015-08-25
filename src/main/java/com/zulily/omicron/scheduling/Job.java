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
package com.zulily.omicron.scheduling;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;

import com.zulily.omicron.EvictingTreeSet;
import com.zulily.omicron.Utils;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;
import com.zulily.omicron.crontab.CrontabExpression;

import com.zulily.omicron.crontab.Schedule;
import org.joda.time.LocalDateTime;

import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

/**
 * ScheduledTasks encapsulates the logic of scheduling a {@link com.zulily.omicron.crontab.CrontabExpression}
 * as well as tracking the external processes as they are launched
 */
public final class Job implements Comparable<Job> {

  private final static AtomicLong JOB_IDS = new AtomicLong();

  private final long jobId = JOB_IDS.incrementAndGet();
  private final CrontabExpression crontabExpression;
  private final Schedule schedule;
  private final String commandLine;
  private final String executingUser;
  private Configuration configuration;
  private final LinkedList<RunningTask> runningTasks = Lists.newLinkedList();
  private final EvictingTreeSet<TaskLogEntry> taskLog = new EvictingTreeSet<>(500, true);
  private final ReentrantLock reentrantLock = new ReentrantLock(true);

  private boolean active = true;

  private int scheduledRunCount = 0;

  private long nextExecutionTimestamp = Utils.DEFAULT_TIMESTAMP;

  /**
   * Constructor
   *
   * @param crontabExpression The associated crontab expression object
   * @param commandLine       The commandLine to execute on the schedule, with variables substituted
   * @param configuration     The potentially overridden configuration to run against
   */
  public Job(final CrontabExpression crontabExpression,
             final String commandLine,
             final Configuration configuration) {

    this.crontabExpression = checkNotNull(crontabExpression, "crontabExpression");
    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.configuration = checkNotNull(configuration, "configuration");
    this.executingUser = crontabExpression.getExecutingUser();
    this.schedule = crontabExpression.createSchedule();
  }

  private boolean shouldRunNow() {

    if (isRunnable()) {

      if (!isActive()) {
        info("{0} skipped execution because it is inactive", commandLine);
        return false;
      }

      if (runningTasks.size() >= configuration.getInt(ConfigKey.TaskMaxInstanceCount)) {
        warn("{0} skipped execution because there are already {1} running", commandLine, String.valueOf(runningTasks.size()));
        return false;
      }

      return true;

    }

    return false;
  }

  /**
   * The primary work routine for scheduled tasks.
   * <p>
   * Evaluates the schedule against the current calendar minute
   * Removes old references to running tasks
   * Calculates the operating statistics of the jobs being launched
   *
   * @return True if a task was launched, False otherwise
   */
  public boolean run() {

    final LocalDateTime localDateTime = LocalDateTime.now(configuration.getChronology());

    // Cleans out old process pointers and log entries
    sweepRunningTasks();

    if (!schedule.timeInSchedule(localDateTime)) {
      return false;
    }

    this.scheduledRunCount++;

    if (shouldRunNow()) {

      final RunningTask runningTask = new RunningTask(scheduledRunCount, commandLine, executingUser, configuration);

      // Most recent run to the start of the list to
      // allow ordered deque from the end of the list
      runningTasks.addFirst(runningTask);

      runningTask.start();

      writeLogEntry(
        new TaskLogEntry(
          runningTask.getTaskId(),
          TaskStatus.Started,
          runningTask.getStartTimeMilliseconds()
        )
      );

      this.nextExecutionTimestamp = this.schedule.getNextRunAfter(localDateTime).toDateTime().getMillis();

      info("[scheduled@{0} {1}] Executing job on line: {2}", localDateTime.toString("yyyyMMdd HH:mm"), configuration.getChronology().getZone().toString(), String.valueOf(crontabExpression.getLineNumber()));

      return true;

    } else {

      writeLogEntry(new TaskLogEntry(this.scheduledRunCount, TaskStatus.Skipped, localDateTime.toDateTime().getMillis()));

    }

    return false;

  }


  private void sweepRunningTasks() {

    // Newer items are added to the start of the list
    // so descending traversal results in ascending chronological
    // order of evaluation
    final int runningTaskCount = runningTasks.size();

    for (int index = runningTaskCount - 1; index >= 0; index--) {

      final RunningTask runningTask = runningTasks.get(index);

      if (runningTask.isDone()) {

        runningTasks.remove(index);

        writeLogEntry(new TaskLogEntry(
          runningTask.getTaskId(),
          runningTask.getTaskStatus(),
          runningTask.getEndTimeMilliseconds()));

      }

    }

  }

  public CrontabExpression getCrontabExpression() {
    return crontabExpression;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(final Configuration configuration) {
    this.configuration = checkNotNull(configuration, "configuration");
  }

  public boolean isRunnable() {
    return !(crontabExpression.isMalformed() || crontabExpression.isCommented());
  }

  public boolean isActive() {
    return active;
  }

  void setActive(boolean active) {
    this.active = active;
  }

  public boolean isRunning() {
    return this.runningTasks.size() > 0;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public int compareTo(Job o) {
    checkNotNull(o, "scheduledTask Compare against null");

    // These are logical 1:1 instances with distinct crontab expressions
    // and are utilized as such
    return this.crontabExpression.compareTo(o.crontabExpression);
  }

  @Override
  public int hashCode() {
    return this.crontabExpression.hashCode();
  }

  @Override
  public boolean equals(Object o) {

    // Involving the config match ensures that config updates
    // will differentiate scheduled tasks as new revisions when the crontab
    // is reloaded
    return o instanceof Job
      && this.crontabExpression.equals(((Job) o).crontabExpression)
      && this.configuration.equals(((Job) o).getConfiguration());
  }

  @Override
  public String toString() {
    return this.crontabExpression.toString();
  }

  public long getNextExecutionTimestamp() {
    return nextExecutionTimestamp;
  }

  public long getJobId() {
    return jobId;
  }

  private void writeLogEntry(final TaskLogEntry taskLogEntry) {
    checkNotNull(taskLogEntry, "taskLogEntry");

    reentrantLock.lock();
    try {
      taskLog.add(taskLogEntry);
    } finally {
      reentrantLock.unlock();
    }
  }

  public ImmutableSortedSet<TaskLogEntry> filterLog(final Set<TaskStatus> statusFilter) {
    checkNotNull(statusFilter, "statusFilter");
    checkArgument(!statusFilter.isEmpty(), "empty filter");

    ImmutableSortedSet.Builder<TaskLogEntry> result = ImmutableSortedSet.naturalOrder();

    reentrantLock.lock();

    try {

      taskLog
        .stream()
        .filter(entry -> statusFilter.contains(entry.getTaskStatus()))
        .map(result::add);

    } finally {
      reentrantLock.unlock();
    }

    return result.build();
  }

  public boolean hasLogEntries() {
    reentrantLock.lock();

    try {
      return !taskLog.isEmpty();
    } finally {
      reentrantLock.unlock();
    }
  }
}
