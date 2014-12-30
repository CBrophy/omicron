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

import com.google.common.collect.Lists;

import com.google.common.collect.Maps;
import com.zulily.omicron.Utils;
import com.zulily.omicron.alert.Alert;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;
import com.zulily.omicron.crontab.CrontabExpression;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.LinkedList;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

/**
 * ScheduledTasks encapsulates the logic of scheduling a {@link com.zulily.omicron.crontab.CrontabExpression}
 * as well as tracking the external processes as they are launched
 */
public final class ScheduledTask implements Comparable<ScheduledTask> {

  private final CrontabExpression crontabExpression;
  private final String commandLine;
  private final String executingUser;
  private Configuration configuration;
  private final LinkedList<RunningTask> runningTasks = Lists.newLinkedList();
  private final TreeMap<String, Alert> policyAlerts = Maps.newTreeMap();

  private boolean active = true;

  private int totalCriticalFailureCount = 0;
  private int totalExpectedFailureCount = 0;
  private int totalSuccessCount = 0;
  private int executionCount = 0;
  private int skippedExecutionCount = 0;

  private long firstExecutionTimestamp = Utils.DEFAULT_TIMESTAMP;
  private long lastSuccessTimestamp = Utils.DEFAULT_TIMESTAMP;
  private long lastExecutionTimestamp = Utils.DEFAULT_TIMESTAMP;

  private int criticalFailuresSinceLastSuccess = 0;
  private int expectedFailuresSinceLastSuccess = 0;

  private long averageSuccessDurationMilliseconds = 0L;
  private long averageExpectedFailureDurationMilliseconds = 0L;
  private long averageCriticalFailureDurationMilliseconds = 0L;

  /**
   * Constructor
   *
   * @param crontabExpression The associated crontab expression object
   * @param commandLine       The commandLine to execute on the schedule, with variables substituted
   * @param configuration     The potentially overridden configuration to run against
   */
  public ScheduledTask(final CrontabExpression crontabExpression,
                       final String commandLine,
                       final Configuration configuration) {

    this.crontabExpression = checkNotNull(crontabExpression, "crontabExpression");
    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.configuration = checkNotNull(configuration, "configuration");
    this.executingUser = crontabExpression.getExecutingUser();

  }

  private boolean shouldRunNow(final LocalDateTime localDateTime) {

    if (crontabExpression.timeInSchedule(localDateTime)) {

      if (!isActive()) {
        info("{0} skipped execution because it is inactive", commandLine);
        return false;
      }

      if (runningTasks.size() >= configuration.getInt(ConfigKey.TaskDuplicateAllowedCount)) {
        warn("{0} skipped execution because there are already {1} running", commandLine, String.valueOf(runningTasks.size()));
        return false;
      }

      return true;

    }

    return false;
  }

  /**
   * The primary work routine for scheduled tasks.
   * <p/>
   * Evaluates the schedule against the current calendar minute
   * Removes old references to running tasks
   * Calculates the operating statistics of the jobs being launched
   *
   * @return True if a task was launched, False otherwise
   */
  public boolean run() {
    final LocalDateTime localDateTime = LocalDateTime.now(configuration.getChronology());

    // Cleans out old process pointers and records stats
    sweepRunningTasks();

    if (shouldRunNow(localDateTime)) {

      this.executionCount++;

      if (this.firstExecutionTimestamp == Utils.DEFAULT_TIMESTAMP) {
        this.firstExecutionTimestamp = DateTime.now().getMillis();
      }

      info("[scheduled@{0} {1}] Executing: {2}", localDateTime.toString("yyyyMMdd HH:mm"), configuration.getChronology().getZone().toString(), commandLine);

      final RunningTask runningTask = new RunningTask(commandLine, executingUser);

      // Most recent run to the start of the list to
      // allow ordered deque from the end of the list
      runningTasks.addFirst(runningTask);

      runningTask.start();

      this.lastExecutionTimestamp = runningTask.getLaunchTimeMilliseconds();

      return true;

    } else {

      this.skippedExecutionCount++;

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

        final long duration = runningTask.getStartTimeMilliseconds() - runningTask.getEndTimeMilliseconds();

        if (runningTask.getReturnCode() == 0) {

          // The task returned a success code

          this.lastSuccessTimestamp = runningTask.getStartTimeMilliseconds();

          this.criticalFailuresSinceLastSuccess = 0;
          this.expectedFailuresSinceLastSuccess = 0;

          this.averageSuccessDurationMilliseconds = rollAverage(
            this.averageSuccessDurationMilliseconds,
            duration,
            ++this.totalSuccessCount
          );

        } else if (runningTask.getReturnCode() < configuration.getInt(ConfigKey.TaskCriticalReturnCode)) {

          // The task returned a code for "expected failure"
          // aka. not notification worthy, but not counted against success rate

          this.expectedFailuresSinceLastSuccess++;

          this.averageExpectedFailureDurationMilliseconds = rollAverage(
            this.averageExpectedFailureDurationMilliseconds,
            duration,
            ++this.totalExpectedFailureCount
          );

        } else {

          // Any other code is considered a critical failure
          // and will result in notification

          this.criticalFailuresSinceLastSuccess++;

          this.averageCriticalFailureDurationMilliseconds = rollAverage(
            this.averageCriticalFailureDurationMilliseconds,
            duration,
            ++this.totalCriticalFailureCount
          );

          info("Scheduled task critical failure detected: {0}", this.toString());
        }

      }

    }

  }

  public int getTotalCriticalFailureCount() {
    return totalCriticalFailureCount;
  }

  public int getTotalExpectedFailureCount() {
    return totalExpectedFailureCount;
  }

  public int getTotalSuccessCount() {
    return totalSuccessCount;
  }

  public long getLastSuccessTimestamp() {
    return lastSuccessTimestamp;
  }

  public long getLastExecutionTimestamp() {
    return lastExecutionTimestamp;
  }

  public int getCriticalFailuresSinceLastSuccess() {
    return criticalFailuresSinceLastSuccess;
  }

  public int getExpectedFailuresSinceLastSuccess() {
    return expectedFailuresSinceLastSuccess;
  }

  public long getAverageSuccessDurationMilliseconds() {
    return averageSuccessDurationMilliseconds;
  }

  public long getAverageExpectedFailureDurationMilliseconds() {
    return averageExpectedFailureDurationMilliseconds;
  }

  public long getAverageCriticalFailureDurationMilliseconds() {
    return averageCriticalFailureDurationMilliseconds;
  }

  public int getSkippedExecutionCount() {
    return skippedExecutionCount;
  }

  public int getExecutionCount() { return executionCount; }

  public long getFirstExecutionTimestamp() {
    return firstExecutionTimestamp;
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

  public TreeMap<String, Alert> getPolicyAlerts() {
    return policyAlerts;
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

  private static long rollAverage(final long currentAverage, final long newValue, final int n) {
    return (newValue + ((n - 1) * currentAverage)) / n;
  }

  @Override
  public int compareTo(ScheduledTask o) {
    checkNotNull(o, "scheduledTask Compare");

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
    return o instanceof ScheduledTask
      && this.crontabExpression.equals(((ScheduledTask) o).crontabExpression)
      && this.configuration.equals(((ScheduledTask) o).getConfiguration());
  }

  @Override
  public String toString() {
    return this.crontabExpression.toString();
  }

}
