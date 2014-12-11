package com.zulily.omicron;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskManager {

  private final Configuration configuration;
  private final ImmutableMap<String, ScheduledTask> scheduledTasks;

  public TaskManager(final Configuration configuration, final ImmutableMap<String, ScheduledTask> scheduledTasks){
    this.configuration = checkNotNull(configuration, "configuration");
    this.scheduledTasks = checkNotNull(scheduledTasks, "scheduledTasks");
  }

  public void run(){

    ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(scheduledTasks.size());

    System.out.println("Kicked off timer at: " + LocalDateTime.now(configuration.getChronology()).toString("yyyyMMdd HH:mm:ss"));

    long targetStart = DateTime.now().plusMinutes(1).withSecondOfMinute(0).withMillisOfSecond(0).getMillis() + 1L;

    for (ScheduledTask scheduledTask : scheduledTasks.values()) {
      scheduledThreadPool.scheduleAtFixedRate(scheduledTask, targetStart - DateTime.now().getMillis(), TimeUnit.MINUTES.toMillis(1), TimeUnit.MILLISECONDS);
    }

    //scheduledThreadPool.shutdown();

    while(true){
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }


}
