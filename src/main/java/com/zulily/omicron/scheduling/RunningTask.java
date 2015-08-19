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

import com.google.common.collect.ComparisonChain;
import com.zulily.omicron.Utils;
import org.joda.time.DateTime;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

/**
 * A running task is a single running instance of a {@link CronJob}
 * which is launched as the specified user using 'su'.
 * <p/>
 * TODO: platform specific
 */
public final class RunningTask implements Runnable, Comparable<RunningTask> {

  private final long launchTimeMilliseconds;
  private final String commandLine;
  private final String executingUser;
  private final Thread thread;
  private final long timeoutMinutes;

  // These values are read by the parent thread to track execution
  private AtomicLong startTimeMilliseconds = new AtomicLong(Long.MAX_VALUE);
  private AtomicLong endTimeMilliseconds = new AtomicLong(-1L);
  private AtomicInteger returnCode = new AtomicInteger(255);
  private AtomicLong pid = new AtomicLong(-1L);

  public RunningTask(final String commandLine,
                     final String executingUser,
                     final long timeoutMinutes) {
    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.executingUser = checkNotNull(executingUser, "executingUser");
    this.launchTimeMilliseconds = DateTime.now().getMillis();
    this.thread = new Thread(this);
    this.timeoutMinutes = timeoutMinutes;
  }

  @Override
  public void run() {
    try {

      if (!canStart()) {
        // TODO: more nuance - there can be other reasons a task cannot start
        warn("Not running as root. Cannot execute: {0}", this.commandLine);
        return;
      }

      final ProcessBuilder processBuilder = new ProcessBuilder("su", "-", executingUser, "-c", commandLine);

      processBuilder.inheritIO();

      this.startTimeMilliseconds.set(DateTime.now().getMillis());

      final Process process = processBuilder.start();

      this.pid.set(determinePid(process));

      // If a timeout is set, then we must enter an isAlive loop test
      // after the kill command is issued otherwise the unkillable
      // process may pile up on the host
      if(timeoutMinutes > 0){

        int killCount = 0;

        while(process.isAlive()) {

          if(killCount > 1) {
            error("{0} attempts to kill process after timeout have failed: {0}", String.valueOf(killCount), commandLine);
          }

          boolean finished = process.waitFor(timeoutMinutes, TimeUnit.MINUTES);

          if (finished) {

            this.returnCode.set(Math.abs(process.exitValue()));

          } else {

            warn("Running process timeout at {0} minutes: {1}", String.valueOf(timeoutMinutes), commandLine);

            process.destroyForcibly();

            killCount++;
          }

        }

      } else {
        this.returnCode.set(Math.abs(process.waitFor()));
      }

      this.endTimeMilliseconds.set(DateTime.now().getMillis());

    } catch (InterruptedException e) {
      warn("Command was interrupted: {0}\ninterruption reason-> {1}", commandLine, e.getMessage());
    } catch (Exception e) {
      error("Command failed: {0}\nerror message-> {1}", commandLine, e.getMessage());
    }
  }

  private static long determinePid(final Process process) {

    /*
    get the PID on unix/linux systems

    Currently, the platform-specific subclasses for java.lang.Process are not accessible. There are bugs open to oracle
    to add an easier way to get this data without such a hack.

    http://bugs.java.com/bugdatabase/view_bug.do;jsessionid=a4867002d972460f4acf3239dc0a?bug_id=4250622

    The dupe request referred to in the bug is no longer accessible

    */


    final Class<? extends Process> processClass = process.getClass();

    if (processClass.getName().equals("java.lang.UNIXProcess")) {

      try {

        final Field pidField = processClass.getDeclaredField("pid");

        pidField.setAccessible(true);

        final Object pidValue = pidField.get(process);

        if (pidValue instanceof Integer) {

          return ((Integer) pidValue).longValue();

        } else if (pidValue instanceof Long) {

          return (Long) pidValue;

        }

      } catch (Throwable e) {
        info("Failed to get pid for task because: {0}", e.getMessage());
      }
    }

    return -1L;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public int compareTo(final RunningTask o) {
    checkNotNull(o, "cannot compare null values");

    // This ordering in intended to be used for the
    // chronologically-dependent loop in ScheduledTask.sweepRunningTasks
    return ComparisonChain.start()
      .compare(this.startTimeMilliseconds.get(), o.startTimeMilliseconds.get())
      .compare(this.launchTimeMilliseconds, o.launchTimeMilliseconds)
      .compare(this.commandLine, o.commandLine)
      .compare(this.pid.get(), o.pid.get())
      .result();
  }

  @Override
  public boolean equals(Object o) {

    // This equivalence test is required to allow
    // RunningTask objects to be safely put into sorted sets/maps
    return o instanceof RunningTask
      && this.commandLine.equals(((RunningTask) o).commandLine)
      && this.pid.get() == ((RunningTask) o).pid.get()
      && this.startTimeMilliseconds.get() == ((RunningTask) o).startTimeMilliseconds.get()
      && this.launchTimeMilliseconds == ((RunningTask) o).launchTimeMilliseconds;
  }

  @Override
  public int hashCode() {
    return this.commandLine.hashCode();
  }

  public int getReturnCode() {
    return returnCode.get();
  }

  public long getStartTimeMilliseconds() {
    return startTimeMilliseconds.get();
  }

  public long getEndTimeMilliseconds() {
    return endTimeMilliseconds.get();
  }

  public boolean isDone() {
    return endTimeMilliseconds.get() > -1L;
  }

  public boolean canStart() {
    return Utils.isRunningAsRoot();
  }

  public void start() {
    this.thread.start();
  }

  public long getPid() {
    return pid.get();
  }

  public long getLaunchTimeMilliseconds() {
    return launchTimeMilliseconds;
  }

}
