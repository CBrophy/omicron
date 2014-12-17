package com.zulily.omicron.scheduling;

import com.google.common.collect.ComparisonChain;
import org.joda.time.DateTime;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

public class RunningTask implements Runnable, Comparable<RunningTask> {

  private final long launchTimeMilliseconds;
  private final String commandLine;
  private final String executingUser;
  private final Thread thread;

  // These values are read by the parent thread to track execution
  private AtomicLong startTimeMilliseconds = new AtomicLong(Long.MAX_VALUE);
  private AtomicLong endTimeMilliseconds = new AtomicLong(-1L);
  private AtomicInteger returnCode = new AtomicInteger(255);
  private AtomicLong pid = new AtomicLong(-1L);

  public RunningTask(final String commandLine, final String executingUser) {
    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.executingUser = checkNotNull(executingUser, "executingUser");
    this.launchTimeMilliseconds = DateTime.now().getMillis();
    this.thread = new Thread(this);
  }

  @Override
  public void run() {
    try {
      final ProcessBuilder processBuilder = new ProcessBuilder("su", "-", executingUser, "-c", commandLine);

      processBuilder.inheritIO();

      this.startTimeMilliseconds.set(DateTime.now().getMillis());

      final Process process = processBuilder.start();

      this.pid.set(determinePid(process));

      this.returnCode.set(Math.abs(process.waitFor()));

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

    return -1;
  }

  @Override
  public int compareTo(final RunningTask o) {
    checkNotNull(o, "cannot compare null values");

    return ComparisonChain.start()
      .compare(this.startTimeMilliseconds.get(), o.startTimeMilliseconds.get())
      .compare(this.launchTimeMilliseconds, o.launchTimeMilliseconds)
      .compare(this.commandLine, o.commandLine)
      .compare(this.pid.get(), o.pid.get())
      .result();
  }

  @Override
  public boolean equals(Object o) {

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