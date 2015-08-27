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
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.COMMA_JOINER;
import static com.zulily.omicron.Utils.COMMA_SPLITTER;
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
  private final Configuration configuration;

  // These values are read by the parent thread to track execution
  private AtomicLong endTimeMilliseconds = new AtomicLong(-1L);
  private AtomicInteger returnCode = new AtomicInteger(255);
  private AtomicLong pid = new AtomicLong(-1L);
  private TaskStatus taskStatus = TaskStatus.FailedStart;

  RunningTask(
    final int taskId,
    final String commandLine,
    final String executingUser,
    final Configuration configuration
  ) {

    this.taskId = taskId;
    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.executingUser = checkNotNull(executingUser, "executingUser");
    this.launchTimeMilliseconds = DateTime.now().getMillis();
    this.thread = new Thread(this);
    this.configuration = configuration;
  }

  @Override
  public void run() {
    try {

      if (!canStart()) {
        // TODO: more nuance - there can be other reasons a task cannot start
        warn("Not running as root. Cannot execute: {0}", this.commandLine);

        this.endTimeMilliseconds.set(DateTime.now().getMillis());

        return;
      }

      final ProcessBuilder processBuilder = new ProcessBuilder(configuration.getString(ConfigKey.CommandSu), "-", executingUser, "-c", commandLine);

      processBuilder.inheritIO();

      final Process process = processBuilder.start();

      this.pid.set(determinePid(process));

      info(
        "PID {0} -> Executed: {1}",
        String.valueOf(getPid()),
        commandLine
      );

      final int timeoutMinutes = configuration.getInt(ConfigKey.TaskTimeoutMinutes);

      // If a timeout is set, then we must enter an isAlive loop test
      // after the kill command is issued otherwise the unkillable
      // process may pile up on the host
      if (timeoutMinutes > 0) {

        int killCount = 0;

        while (process.isAlive()) {

          if (killCount > 1) {
            error(
              "{0} attempts to kill process after timeout have failed: {0}",
              String.valueOf(killCount),
              commandLine
            );
          }

          boolean finished = process.waitFor(timeoutMinutes, TimeUnit.MINUTES);

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
    return Utils.isRunningAsRoot();
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

      final List<String> pidList = getPidList();

      warn(
        "Task timeout after {0} minutes. Killing PID tree [{1}]: {2}",
        configuration.getString(ConfigKey.TaskTimeoutMinutes),
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
      for (String pid : pidList) {
        new ProcessBuilder(configuration.getString(ConfigKey.CommandKill), "-9", pid).start();
      }

    } else {
      warn("Could not kill task {0} since the pid could not be retrieved", commandLine);
    }
  }

  private List<String> getPidList() throws IOException, InterruptedException {
    final Process pidListProcess = new ProcessBuilder(configuration.getString(ConfigKey.CommandPstree), String.valueOf(getPid()), "-p", "-a", "-l")
      .start();

    final BufferedReader input = new BufferedReader(
      new
        InputStreamReader(pidListProcess.getInputStream())
    );

    final BufferedReader error = new BufferedReader(
      new
        InputStreamReader(pidListProcess.getErrorStream())
    );

    final List<String> results = new ArrayList<>();

    String line;

    while ((line = input.readLine()) != null) {
      String command = COMMA_SPLITTER.splitToList(line).get(1);

      results.add(command.substring(0, command.indexOf(' ')));
    }

    while ((line = error.readLine()) != null) {
      error("Error getting pid list for pid {0}: {1}", String.valueOf(getPid()), line);
    }

    pidListProcess.waitFor();

    input.close();
    error.close();

    return results;
  }
}
