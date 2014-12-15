package com.zulily.omicron;

import com.google.common.collect.Lists;

import com.zulily.omicron.crontab.CrontabExpression;

import org.joda.time.LocalDateTime;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduledTask implements Runnable, Comparable<ScheduledTask> {
  private final CrontabExpression crontabExpression;
  private final String commandLine;
  private final String executingUser;
  private final Configuration configuration;

  private boolean active = true;

  // These stats are going to be read by the parent thread for notification
  // and logging, so they need to have a level of concurrency protection
  private AtomicInteger totalCriticalFailureCount = new AtomicInteger(0);
  private AtomicInteger totalExpectedFailureCount = new AtomicInteger(0);
  private AtomicInteger totalSuccessCount = new AtomicInteger(0);
  private AtomicInteger executionCount = new AtomicInteger(0);
  private AtomicInteger skippedExecutionCount = new AtomicInteger(0);

  private AtomicLong lastSuccessTimestamp = new AtomicLong(0L);
  private AtomicLong lastExecutionTimestamp = new AtomicLong(0L);

  private AtomicInteger criticalFailuresSinceLastSuccess = new AtomicInteger(0);
  private AtomicInteger expectedFailuresSinceLastSuccess = new AtomicInteger(0);

  private AtomicLong averageSuccessDurationMilliseconds = new AtomicLong(0L);
  private AtomicLong averageExpectedFailureDurationMilliseconds = new AtomicLong(0L);
  private AtomicLong averageCriticalFailureDurationMilliseconds = new AtomicLong(0L);

  private LinkedList<RunningTask> runningTasks = Lists.newLinkedList();

  public ScheduledTask(final CrontabExpression crontabExpression,
                       final String commandLine,
                       final Configuration configuration) {
    this.crontabExpression = checkNotNull(crontabExpression, "crontabExpression");
    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.configuration = checkNotNull(configuration, "configuration");
    this.executingUser = crontabExpression.getExecutingUser();
  }

  public boolean shouldRunNow(final LocalDateTime localDateTime) {

    return isActive()
      && crontabExpression.getDaysOfWeek().contains(localDateTime.getDayOfWeek() == 7 ? 0 : localDateTime.getDayOfWeek())
      && crontabExpression.getMonths().contains(localDateTime.getMonthOfYear())
      && crontabExpression.getDays().contains(localDateTime.getDayOfMonth())
      && crontabExpression.getHours().contains(localDateTime.getHourOfDay())
      && crontabExpression.getMinutes().contains(localDateTime.getMinuteOfHour())
      && runningTasks.size() < configuration.getTaskDuplicateAllowedCount();

  }

  @Override
  public void run() {
    final LocalDateTime localDateTime = LocalDateTime.now(configuration.getChronology());

    // Cleans out old process pointers and records stats
    sweepRunningTasks();

    if (shouldRunNow(localDateTime)) {

      this.executionCount.getAndIncrement();

      System.out.println(String.format("[%s] Executing: %s", localDateTime.toString("yyyyMMdd HH:mm"), commandLine));

      RunningTask runningTask = new RunningTask(commandLine, executingUser);

      // Most recent run to the start of the list to
      // allow ordered deque from the end of the list
      runningTasks.addFirst(runningTask);

      runningTask.start();

      this.lastExecutionTimestamp.set(runningTask.getLaunchTimeMilliseconds());

    } else {

      this.skippedExecutionCount.getAndIncrement();

      System.out.println(String.format("[%s] Would NOT execute: %s", localDateTime.toString("yyyyMMdd HH:mm"), commandLine));
    }
  }

  private void sweepRunningTasks() {

    // Newer items are added to the start of the list
    // so descending traversal results in ascending chronological
    // order of evaluation
    int runningTaskCount = runningTasks.size();

    for(int index = runningTaskCount - 1; index >= 0; index--){

      RunningTask runningTask = runningTasks.get(index);

      if (runningTask.isDone()) {

        runningTasks.remove(index);

        long duration = runningTask.getStartTimeMilliseconds() - runningTask.getEndTimeMilliseconds();

        if (runningTask.getReturnCode() == 0) {

          // The task returned a success code

          this.lastSuccessTimestamp.set(runningTask.getStartTimeMilliseconds());

          this.criticalFailuresSinceLastSuccess.set(0);
          this.expectedFailuresSinceLastSuccess.set(0);

          this.averageSuccessDurationMilliseconds.set(rollAverage(
            this.averageSuccessDurationMilliseconds.get(),
            duration,
            this.totalSuccessCount.incrementAndGet()
          ));

        } else if (runningTask.getReturnCode() < configuration.getTaskReturnCodeCriticalFailureThreshold()) {

          // The task returned a code for "expected failure"
          // aka. not notification worthy, but not counted against success rate

          this.expectedFailuresSinceLastSuccess.getAndIncrement();

          this.averageExpectedFailureDurationMilliseconds.set(rollAverage(
            this.averageExpectedFailureDurationMilliseconds.get(),
            duration,
            this.totalExpectedFailureCount.incrementAndGet()
          ));

        } else {

          // Any other code is considered a critical failure
          // and will result in notification

          this.criticalFailuresSinceLastSuccess.getAndIncrement();

          this.averageCriticalFailureDurationMilliseconds.set(rollAverage(
            this.averageCriticalFailureDurationMilliseconds.get(),
            duration,
            this.totalCriticalFailureCount.incrementAndGet()
          ));

        }

      }

    }

  }

  public int getTotalCriticalFailureCount() {
    return totalCriticalFailureCount.get();
  }

  public int getTotalExpectedFailureCount() {
    return totalExpectedFailureCount.get();
  }

  public int getTotalSuccessCount() {
    return totalSuccessCount.get();
  }

  public long getLastSuccessTimestamp() {
    return lastSuccessTimestamp.get();
  }

  public long getLastExecutionTimestamp() {
    return lastExecutionTimestamp.get();
  }

  public int getCriticalFailuresSinceLastSuccess() {
    return criticalFailuresSinceLastSuccess.get();
  }

  public int getExpectedFailuresSinceLastSuccess() {
    return expectedFailuresSinceLastSuccess.get();
  }

  public long getAverageSuccessDurationMilliseconds() {
    return averageSuccessDurationMilliseconds.get();
  }

  public long getAverageExpectedFailureDurationMilliseconds() {
    return averageExpectedFailureDurationMilliseconds.get();
  }

  public long getAverageCriticalFailureDurationMilliseconds() {
    return averageCriticalFailureDurationMilliseconds.get();
  }

  public int getSkippedExecutionCount() {
    return skippedExecutionCount.get();
  }

  public int getExecutionCount() { return executionCount.get(); }

  boolean isActive() {
    return active;
  }

  void setActive(boolean active) {
    this.active = active;
  }

  public boolean isRunning(){
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
  public int hashCode(){
    return this.crontabExpression.hashCode();
  }

  @Override
  public boolean equals(Object o){
    return o instanceof ScheduledTask
      && this.crontabExpression.equals(((ScheduledTask) o).crontabExpression);
  }

  @Override
  public String toString(){
    return this.crontabExpression.toString();
  }

}
