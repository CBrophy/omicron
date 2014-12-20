package com.zulily.omicron.scheduling;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zulily.omicron.alert.AlertManager;
import com.zulily.omicron.conf.Configuration;
import com.zulily.omicron.crontab.Crontab;
import com.zulily.omicron.crontab.CrontabExpression;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

/**
 * Primary omicron engine class that launches task evaluation once every calendar minute.
 */
public class TaskManager {

  private final Configuration configuration;
  private final ArrayList<ScheduledTask> retiredScheduledTasks = Lists.newArrayList();
  private final AlertManager alertManager;
  private HashSet<ScheduledTask> scheduledTaskSet = Sets.newHashSet();

  /**
   * Constructor
   *
   * @param configuration the primary configuration loaded either from the default conf file, or from
   * the set of defaults in the {@link com.zulily.omicron.conf.Configuration} class
   */
  public TaskManager(final Configuration configuration) {
    this.configuration = checkNotNull(configuration, "configuration");
    this.alertManager = new AlertManager(configuration.getAlertEmail());
  }

  /**
   * The main "work" routine in Omicron
   *
   * Blocks and wakes each second to check for and load updates from
   * the crontab file, or to launch tasks that are scheduled to execute
   * at the start of the current calendar minute
   *
   * @throws InterruptedException
   */
  public void run() throws InterruptedException {
    // The minute logic is intended to stay calibrated
    // with the current calendar minute, with the intent being
    // to run the scheduled jobs as close to second-of-minute 0 as possible
    // while minimizing acquired execution drift over time

    long lastModifiedCurrentCrontab = Long.MIN_VALUE;

    // Optimistically assume that we'll be waking on the
    // next calendar minute
    long targetExecuteMinute = getMinuteMillisFromNow(1);

    Crontab crontab = null;

    //noinspection InfiniteLoopStatement
    while (true) {

      long currentExecuteMinute = getMinuteMillisFromNow(0);

      // Trigger when the execute minute comes up or is past-due
      // until then watch for crontab changes or sleep
      while (currentExecuteMinute < targetExecuteMinute) {

        // If the crontab has been changed - re-evaluate the scheduled tasks
        if (configuration.getCrontab().lastModified() != lastModifiedCurrentCrontab) {

          crontab = new Crontab(configuration);

          lastModifiedCurrentCrontab = crontab.getLastModified();

          if (crontab.getBadRowCount() > 0) {
            //TODO: report these
            warn("Bad rows found in crontab! See error log for details");
          }

          consumeCronChanges(crontab);
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        currentExecuteMinute = getMinuteMillisFromNow(0);
      }

      // This admits the possibility that, due to drift or
      // due to the length of time it takes to read crontab changes,
      // we may actually pass a calendar minute without evaluation
      // of the scheduled task list
      //
      // The current implementation of crond never evaluates the current minute
      // that it detects schedule changes. For now I'm calling that
      // particular case "expected behavior"
      if (currentExecuteMinute != targetExecuteMinute) {
        warn("Scheduled tasks may have been missed due to missed minute target {0}", String.valueOf(targetExecuteMinute));
      }

      // Set for re-evaluation in the next calendar minute
      targetExecuteMinute = getMinuteMillisFromNow(1);

      try {

        final long taskEvaluationStartMs = DateTime.now().getMillis();

        int executeCount = 0;

        for (final ScheduledTask scheduledTask : scheduledTaskSet) {

          if(scheduledTask.run()){
            executeCount++;
          }

        }
        if(executeCount > 0) {
          info("Task evaluation took {0} ms: running {1} task(s)", String.valueOf(DateTime.now().getMillis() - taskEvaluationStartMs), String.valueOf(executeCount));
        }

        // Tasks that have been removed from execution due to a crontab
        // update will remain referenced until all processes associated with the
        // old task return
        retireOldTasks();

        // Perform the alert evaluation outside of the task launching loop
        // to avoid delaying task launch and to skip evaluation
        // against retired tasks
        alertManager.sendAlerts(scheduledTaskSet);

      } catch (Exception e) {
        // The only acceptable expected end to omicron is by being killed as a process
        // or via a bad conf file
        error("Task evaluation exception\n{0}", Throwables.getStackTraceAsString(e));
      }
    }

  }

  private void consumeCronChanges(final Crontab crontab) {
    info("Reading crontab...");

    final HashSet<ScheduledTask> result = Sets.newHashSet();

    final HashSet<ScheduledTask> scheduledTaskUpdates = Sets.newHashSet();

    for (final CrontabExpression crontabExpression : crontab.getCrontabExpressions()) {
      final Configuration configurationOverride = crontab.getConfigurationOverrides().get(crontabExpression.getLineNumber());

      final ScheduledTask scheduledTask = new ScheduledTask(
        crontabExpression,
        substituteVariables(crontabExpression.getCommand(), crontab.getVariables()),
        configurationOverride == null ? configuration : configurationOverride);

      scheduledTaskUpdates.add(scheduledTask);
    }

    // This is a view containing old scheduled tasks that have been removed or
    // reconfigured
    final Sets.SetView<ScheduledTask> oldScheduledTasks = Sets.difference(scheduledTaskSet, scheduledTaskUpdates);

    info("CRON UPDATE: {0} tasks no longer scheduled or out of date", String.valueOf(oldScheduledTasks.size()));

    // This is a view of scheduled tasks that will not be updated by the cron reload
    final Sets.SetView<ScheduledTask> existingScheduledTasks = Sets.intersection(scheduledTaskSet, scheduledTaskUpdates);

    info("CRON UPDATE: {0} tasks unchanged", String.valueOf(existingScheduledTasks.size()));

    // This is a view of scheduled tasks that are new or have been changed
    final Sets.SetView<ScheduledTask> newScheduledTasks = Sets.difference(scheduledTaskUpdates, scheduledTaskSet);

    info("CRON UPDATE: {0} tasks are new or updated", String.valueOf(newScheduledTasks.size()));

    // Add all new tasks
    // keep references to old tasks that are still running
    // and transfer instances that haven't changed
    result.addAll(newScheduledTasks);

    for (final ScheduledTask scheduledTask : scheduledTaskSet) {

      if (oldScheduledTasks.contains(scheduledTask) && scheduledTask.isRunning()) {

        scheduledTask.setActive(false);
        result.add(scheduledTask);

        retiredScheduledTasks.add(scheduledTask);
      }

      if (existingScheduledTasks.contains(scheduledTask)) {

        if (!scheduledTask.isActive()) {
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

  private void retireOldTasks() {
    final int retiredTaskCount = retiredScheduledTasks.size() - 1;

    for (int index = retiredTaskCount; index >= 0; index--) {
      final ScheduledTask retiredTask = retiredScheduledTasks.get(index);

      if (!retiredTask.isRunning()) {
        info("Retiring inactive task: {0}", retiredTask.toString());
        retiredScheduledTasks.remove(index);
        scheduledTaskSet.remove(retiredTask);
      }
    }
  }

  private static String substituteVariables(final String line, final Map<String, String> variableMap) {
    String substituted = line;

    for (final Map.Entry<String, String> variableEntry : variableMap.entrySet()) {
      substituted = substituted.replace(variableEntry.getKey(), variableEntry.getValue());
    }

    return substituted;
  }

  private static long getMinuteMillisFromNow(final int minuteIncrement) {
    return DateTime.now().plusMinutes(minuteIncrement).withSecondOfMinute(0).withMillisOfSecond(0).getMillis();
  }

}
