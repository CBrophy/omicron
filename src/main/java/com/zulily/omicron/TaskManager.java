package com.zulily.omicron;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.zulily.omicron.crontab.Crontab;
import com.zulily.omicron.crontab.CrontabExpression;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskManager {

  private final Configuration configuration;
  private HashSet<ScheduledTask> scheduledTaskSet = Sets.newHashSet();

  public TaskManager(final Configuration configuration) {
    this.configuration = checkNotNull(configuration, "configuration");
  }

  public void run() throws InterruptedException {
    // The minute logic is intended to stay calibrated
    // with the current minute to run the scheduled
    // jobs as close to second-of-minute 0 as possible
    // while minimizing for execution drift over time

    long lastModified = Long.MIN_VALUE;

    // Optimistically assume that we'll be running on the
    // next calendar minute
    long executeMinute = getCurrentExecuteMinute() + 1;

    Crontab crontab = null;

    while (true) {

      long currentExecuteMinute = getCurrentExecuteMinute();

      // Trigger when the execute minute comes up or is past-due
      // until then watch for crontab changes or sleep
      while(currentExecuteMinute < executeMinute) {

        // If the crontab has been changed - re-evaluate the scheduled tasks
        if(configuration.getCrontab().lastModified() != lastModified){

          crontab = new Crontab(configuration);

          lastModified = crontab.getLastModified();

          if(crontab.getBadRowCount() > 0){
            System.out.println("Bad rows found in crontab! See error log for details");
          }

          updateScheduledTasks(crontab);
        }


        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        currentExecuteMinute = getCurrentExecuteMinute();
      }

      if(currentExecuteMinute != executeMinute){
        System.out.println("WARN: Scheduled tasks may have been missed!");
      }

      // Schedule re-evaluation in the next calendar minute
      executeMinute = getCurrentExecuteMinute() + 1;

      try {

        long taskEvaluationStartMs = DateTime.now().getMillis();

        for (ScheduledTask scheduledTask : scheduledTaskSet) {
          scheduledTask.run();
        }

        System.out.printf("Task evaluation took %s ms", DateTime.now().getMillis() - taskEvaluationStartMs);

      } catch (Exception e) {
        System.out.println("Task evaluation exception");
        System.out.println(Throwables.getStackTraceAsString(e));
      }
    }

  }

  private void updateScheduledTasks(final Crontab crontab) {
    System.out.println("Reading crontab...");

    HashSet<ScheduledTask> result = Sets.newHashSet();

    HashSet<ScheduledTask> scheduledTaskUpdates = Sets.newHashSet();

    for (CrontabExpression crontabExpression : crontab.getCrontabExpressions()) {

      ScheduledTask scheduledTask = new ScheduledTask(
        crontabExpression,
        substituteVariables(crontabExpression.getCommand(), crontab.getVariables()),
        configuration);

      scheduledTaskUpdates.add(scheduledTask);
    }

    // This is a view containing old scheduled tasks that have been removed or
    // reconfigured
    Sets.SetView<ScheduledTask> oldScheduledTasks = Sets.difference(scheduledTaskSet, scheduledTaskUpdates);

    System.out.printf("UPDATE: %s tasks no longer scheduled or out of date\n", oldScheduledTasks.size());

    // This is a view of scheduled tasks that will not be updated by the cron reload
    Sets.SetView<ScheduledTask> existingScheduledTasks = Sets.intersection(scheduledTaskSet, scheduledTaskUpdates);

    System.out.printf("UPDATE: %s tasks unchanged\n", existingScheduledTasks.size());

    // This is a view of scheduled tasks that are new or have been changed
    Sets.SetView<ScheduledTask> newScheduledTasks = Sets.difference(scheduledTaskUpdates, scheduledTaskSet);

    System.out.printf("UPDATE: %s tasks are new or updated\n", newScheduledTasks.size());

    // Add all new tasks
    // keep references to old tasks that are still running
    // and transfer instances that haven't changed
    result.addAll(newScheduledTasks);

    for (ScheduledTask scheduledTask : scheduledTaskSet) {

      if(oldScheduledTasks.contains(scheduledTask) && scheduledTask.isRunning()){
        scheduledTask.setActive(false);
        result.add(scheduledTask);
      }

      if(existingScheduledTasks.contains(scheduledTask)){

        if(!scheduledTask.isActive()){
          // Did someone re-add something that was running and then removed?
          // For whatever reason, it's now set to run again so just re-activate the instance
          System.out.printf("UPDATE: Reactivating %s\n", scheduledTask.toString());
          scheduledTask.setActive(true);
        }

        result.add(scheduledTask);
      }
    }

    this.scheduledTaskSet = result;
  }


  private static String substituteVariables(final String line, final Map<String, String> variableMap){
    String substituted = line;
    for (Map.Entry<String, String> variableEntry : variableMap.entrySet()) {
      substituted = substituted.replace(variableEntry.getKey(), variableEntry.getValue());
    }
    return substituted;
  }

  private static long getCurrentExecuteMinute(){
    return DateTime.now().withSecondOfMinute(0).withMillisOfSecond(0).getMillis();
  }

}
