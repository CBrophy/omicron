package com.zulily.omicron;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zulily.omicron.crontab.Crontab;
import com.zulily.omicron.crontab.CrontabExpression;
import com.zulily.omicron.sla.Policy;
import com.zulily.omicron.sla.TimeSinceLastSuccess;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

public class TaskManager {
  private final ImmutableList<Policy> slaPolicies;
  private final Configuration configuration;
  private HashSet<ScheduledTask> scheduledTaskSet = Sets.newHashSet();
  private ArrayList<ScheduledTask> retiredScheduledTasks = Lists.newArrayList();


  public TaskManager(final Configuration configuration) {
    this.configuration = checkNotNull(configuration, "configuration");
    this.slaPolicies = ImmutableList.of((Policy)new TimeSinceLastSuccess(configuration));
  }

  public void run() throws InterruptedException {
    // The minute logic is intended to stay calibrated
    // with the current minute to run the scheduled
    // jobs as close to second-of-minute 0 as possible
    // while minimizing for execution drift over time

    long lastModified = Long.MIN_VALUE;

    // Optimistically assume that we'll be running on the
    // next calendar minute
    long executeMinute = getCurrentExecuteMinute() + TimeUnit.MINUTES.toMillis(1);

    Crontab crontab = null;

    //noinspection InfiniteLoopStatement
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
            warn("Bad rows found in crontab! See error log for details");
          }

          consumeCronChanges(crontab);
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        currentExecuteMinute = getCurrentExecuteMinute();
      }

      if(currentExecuteMinute != executeMinute){
        warn("Scheduled tasks may have been missed due to missed minute target {0}", String.valueOf(executeMinute));
      }

      // Schedule re-evaluation in the next calendar minute
      executeMinute = getCurrentExecuteMinute() + TimeUnit.MINUTES.toMillis(1);

      try {

        long taskEvaluationStartMs = DateTime.now().getMillis();

        for (ScheduledTask scheduledTask : scheduledTaskSet) {
          scheduledTask.run();
        }

        info("Task evaluation took {0} ms", String.valueOf(DateTime.now().getMillis() - taskEvaluationStartMs));

        checkTaskPolicies();

        retireOldTasks();

      } catch (Exception e) {
        error("Task evaluation exception\n{0}",Throwables.getStackTraceAsString(e));
      }
    }

  }

  private void consumeCronChanges(final Crontab crontab) {
    info("Reading crontab...");

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

    info("CRON UPDATE: {0} tasks no longer scheduled or out of date", String.valueOf(oldScheduledTasks.size()));

    // This is a view of scheduled tasks that will not be updated by the cron reload
    Sets.SetView<ScheduledTask> existingScheduledTasks = Sets.intersection(scheduledTaskSet, scheduledTaskUpdates);

    info("CRON UPDATE: {0} tasks unchanged", String.valueOf(existingScheduledTasks.size()));

    // This is a view of scheduled tasks that are new or have been changed
    Sets.SetView<ScheduledTask> newScheduledTasks = Sets.difference(scheduledTaskUpdates, scheduledTaskSet);

    info("CRON UPDATE: {0} tasks are new or updated", String.valueOf(newScheduledTasks.size()));

    // Add all new tasks
    // keep references to old tasks that are still running
    // and transfer instances that haven't changed
    result.addAll(newScheduledTasks);

    for (ScheduledTask scheduledTask : scheduledTaskSet) {

      if(oldScheduledTasks.contains(scheduledTask) && scheduledTask.isRunning()){

        scheduledTask.setActive(false);
        result.add(scheduledTask);

        retiredScheduledTasks.add(scheduledTask);
      }

      if(existingScheduledTasks.contains(scheduledTask)){

        if(!scheduledTask.isActive()){
          // Did someone re-add a task that was running and then removed?
          // For whatever reason, it's now set to run again so just re-activate the instance
          info("CRON UPDATE: Reactivating {0}", scheduledTask.toString());
          scheduledTask.setActive(true);
        }

        result.add(scheduledTask);
      }
    }

    this.scheduledTaskSet = result;
  }

  private void checkTaskPolicies(){
    for (ScheduledTask scheduledTask : scheduledTaskSet) {

      if(scheduledTask.isActive()){
        continue;
      }

      for (Policy slaPolicy : slaPolicies) {
        if(slaPolicy.enabled()){
          slaPolicy.evaluate(scheduledTask);
        }
      }

    }
  }

  private void retireOldTasks(){
    int retiredTaskCount = retiredScheduledTasks.size() - 1;

    for(int index= retiredTaskCount; index >= 0; index--){
      ScheduledTask retiredTask = retiredScheduledTasks.get(index);

      if(!retiredTask.isRunning()){
        info("Retiring inactive task: {0}", retiredTask.toString());
        retiredScheduledTasks.remove(index);
        scheduledTaskSet.remove(retiredTask);
      }
    }
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
