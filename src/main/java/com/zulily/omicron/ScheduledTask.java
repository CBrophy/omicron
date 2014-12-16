package com.zulily.omicron;

import com.google.common.collect.Lists;

import com.zulily.omicron.crontab.CrontabExpression;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

public class ScheduledTask implements Comparable<ScheduledTask> {
  private final CrontabExpression crontabExpression;
  private final String commandLine;
  private final String executingUser;
  private final Configuration configuration;

  private boolean active = true;

  // These stats are going to be read by the parent thread for notification
  // and logging, so they need to have a level of concurrency protection
  private int totalCriticalFailureCount = 0;
  private int totalExpectedFailureCount = 0;
  private int totalSuccessCount = 0;
  private int executionCount = 0;
  private int skippedExecutionCount = 0;

  private long firstExecutionTimestamp = 0L;
  private long lastSuccessTimestamp = 0L;
  private long lastExecutionTimestamp = 0L;

  private int criticalFailuresSinceLastSuccess = 0;
  private int expectedFailuresSinceLastSuccess = 0;

  private long averageSuccessDurationMilliseconds = 0L;
  private long averageExpectedFailureDurationMilliseconds = 0L;
  private long averageCriticalFailureDurationMilliseconds = 0L;

  private LinkedList<RunningTask> runningTasks = Lists.newLinkedList();

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

      if (runningTasks.size() >= configuration.getTaskDuplicateAllowedCount()) {
        warn("{0} skipped execution because there are already {1} running", commandLine, String.valueOf(runningTasks.size()));
        return false;
      }

      return true;

    }

    return false;
  }

  public void run() {
    final LocalDateTime localDateTime = LocalDateTime.now(configuration.getChronology());

    // Cleans out old process pointers and records stats
    sweepRunningTasks();

    if (shouldRunNow(localDateTime)) {

      this.executionCount++;

      if (this.firstExecutionTimestamp == 0L) {
        this.firstExecutionTimestamp = DateTime.now().getMillis();
      }

      info("[scheduled@{0}] Executing: {1}", localDateTime.toString("yyyyMMdd HH:mm"), commandLine);

      final RunningTask runningTask = new RunningTask(commandLine, executingUser);

      // Most recent run to the start of the list to
      // allow ordered deque from the end of the list
      runningTasks.addFirst(runningTask);

      runningTask.start();

      this.lastExecutionTimestamp = runningTask.getLaunchTimeMilliseconds();

    } else {

      this.skippedExecutionCount++;

    }
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

        } else if (runningTask.getReturnCode() < configuration.getTaskReturnCodeCriticalFailureThreshold()) {

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

  boolean isActive() {
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
    return o instanceof ScheduledTask
      && this.crontabExpression.equals(((ScheduledTask) o).crontabExpression);
  }

  @Override
  public String toString() {
    return this.crontabExpression.toString();
  }
}
