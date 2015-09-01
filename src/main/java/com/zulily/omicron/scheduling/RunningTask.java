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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.zulily.omicron.Utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.COMMA_JOINER;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

/**
 * A running task is a single running instance of a {@link Job}
 * which is launched as the specified user using 'su'.
 * <p>
 * TODO: platform specific
 */
final class RunningTask implements Runnable, Comparable<RunningTask> {

  private final long launchTimeMilliseconds;
  private final String commandLine;
  private final String executingUser;
  private final Thread thread;
  private final int taskId;
  private final int taskTimeoutMinutes;
  private final String suCommand;
  private final String killCommand;

  // These values are read by the parent thread to track execution
  private AtomicLong endTimeMilliseconds = new AtomicLong(-1L);
  private AtomicInteger returnCode = new AtomicInteger(255);
  private AtomicLong pid = new AtomicLong(-1L);
  private TaskStatus taskStatus = TaskStatus.FailedStart;

  RunningTask(
    final int taskId,
    final String commandLine,
    final String executingUser,
    final int taskTimeoutMinutes,
    final String suCommand,
    final String killCommand
  ) {

    this.taskId = taskId;
    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.executingUser = checkNotNull(executingUser, "executingUser");
    this.suCommand = checkNotNull(suCommand, "suCommand");
    this.killCommand = checkNotNull(killCommand, "killCommand");
    this.launchTimeMilliseconds = Clock.systemUTC().millis();
    this.thread = new Thread(this);
    this.taskTimeoutMinutes = taskTimeoutMinutes;
  }

  @Override
  public void run() {
    try {

      if (!Utils.isRunningAsRoot()) {

        warn("Not running as root. Cannot execute: {0}", this.commandLine);

        this.endTimeMilliseconds.set(Clock.systemUTC().millis());

        return;
      }

      if (!Utils.fileExistsAndCanRead(suCommand)) {

        warn("su command does not exist as specified location: {0}", this.suCommand);

        this.endTimeMilliseconds.set(Clock.systemUTC().millis());

        return;
      }

      if (!Utils.fileExistsAndCanRead(killCommand)) {

        warn("kill command does not exist as specified location: {0}", this.killCommand);

        this.endTimeMilliseconds.set(Clock.systemUTC().millis());

        return;
      }


      final ProcessBuilder processBuilder = new ProcessBuilder(suCommand, "-", executingUser, "-c", commandLine);

      processBuilder.inheritIO();

      final Process process = processBuilder.start();

      this.pid.set(determinePid(process));

      info(
        "PID {0} -> STARTED: {1}",
        String.valueOf(getPid()),
        commandLine
      );

      // If a timeout is set, then we must enter an isAlive loop test
      // after the kill command is issued otherwise the unkillable
      // process may pile up on the host
      if (taskTimeoutMinutes > 0) {

        int killCount = 0;

        while (process.isAlive()) {

          if (killCount > 1) {
            error(
              "{0} attempts to kill process after timeout have failed: {0}",
              String.valueOf(killCount),
              commandLine
            );
          }

          boolean finished = process.waitFor(taskTimeoutMinutes, TimeUnit.MINUTES);

          if (finished) {

            this.returnCode.set(Math.abs(process.exitValue()));

          } else {

            kill();

            this.taskStatus = TaskStatus.Killed;

            killCount++;
          }

        }

      } else {
        this.returnCode.set(Math.abs(process.waitFor()));
      }

      // Don't overwrite killed state
      if (taskStatus == TaskStatus.FailedStart) {
        this.taskStatus = this.getReturnCode() == 0 ? TaskStatus.Complete : TaskStatus.Error;
      }

      this.endTimeMilliseconds.set(Clock.systemUTC().millis());

      info(
        "PID {0} -> TERMINATED: {1} [duration of {2} minutes]",
        String.valueOf(getPid()),
        commandLine,
        String.valueOf(TimeUnit.MILLISECONDS.toMinutes(this.getEndTimeMilliseconds() - this.launchTimeMilliseconds))
      );


    } catch (InterruptedException e) {
      warn("Command was interrupted: {0}\ninterruption reason-> {1}", commandLine, e.getMessage());
    } catch (Exception e) {
      error("Command failed: {0}\nerror message-> {1}", commandLine, e.getMessage());
    }
  }

  static long determinePid(final Process process) {

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
      && this.launchTimeMilliseconds == ((RunningTask) o).launchTimeMilliseconds;
  }

  @Override
  public int hashCode() {
    return this.commandLine.hashCode();
  }

  public int getReturnCode() {
    return returnCode.get();
  }

  public long getEndTimeMilliseconds() {
    return endTimeMilliseconds.get();
  }

  public boolean isDone() {
    return endTimeMilliseconds.get() > -1L;
  }

  private boolean canStart() {
    return Utils.isRunningAsRoot()
      && (new File(killCommand)).exists()
      && (new File(suCommand)).exists();
  }

  public void start() {
    this.thread.start();
  }

  public long getPid() {
    return pid.get();
  }

  public int getTaskId() {
    return taskId;
  }

  public TaskStatus getTaskStatus() {
    return taskStatus;
  }

  private void kill() throws IOException, InterruptedException {
    if (getPid() > -1L) {
      // The JVM cannot kill children of child processes - only external kill will work

      final Set<Long> pidList = recursivelyFindAllChildren(getPid());

      warn(
        "Task timeout after {0} minutes. Killing PID tree [{1}]: {2}",
        String.valueOf(taskTimeoutMinutes),
        COMMA_JOINER.join(pidList),
        commandLine
      );

      // Throw a kill -9 at the task and all of its children
      //
      // Note: there is SOME risk that this will kill the wrong process.
      //
      // A PID can be recycled by the OS in the time it takes to kill a process
      // and it's remaining children. The likelihood is low, as most OSes attempt
      // to avoid recycling PIDs so quickly, but the risk is there.
      for (Long pid : pidList) {
        new ProcessBuilder(killCommand, "-9", String.valueOf(pid)).start();
      }

    } else {
      warn("Could not kill task {0} since the pid could not be retrieved", commandLine);
    }
  }

  private static Set<Long> getProcFsChildren(long pid) {
    try {
      File childrenFile = new File(String.format("/proc/%s/task/%s/children", pid, pid));

      HashSet<Long> result = Sets.newHashSet();

      if (Utils.fileExistsAndCanRead(childrenFile)) {

        Files.readLines(childrenFile, Charset.defaultCharset())
          .stream()
          .filter(line -> !Utils.isNullOrEmpty(line))
          .map(Utils.WHITESPACE_SPLITTER::splitToList)
          .forEach(list -> list
            .stream()
            .map(Long::parseLong)
            .forEach(result::add));
      }

      return result;

    } catch (Exception ignored) {
    }

    return ImmutableSet.of();
  }

  static Set<Long> recursivelyFindAllChildren(final long pid) {

    Set<Long> result = Sets.newHashSet();

    Set<Long> immediateChildren = getProcFsChildren(pid);

    result.addAll(immediateChildren);

    immediateChildren
      .forEach(child -> result.addAll(recursivelyFindAllChildren(child)));

    result.add(pid);

    return result;

  }

}
