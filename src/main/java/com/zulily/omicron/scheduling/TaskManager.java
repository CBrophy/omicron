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

import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;

/**
 * Primary omicron engine class that launches task evaluation once every calendar minute.
 */
public class TaskManager {
  private final ArrayList<ScheduledTask> retiredScheduledTasks = Lists.newArrayList();
  private HashSet<ScheduledTask> scheduledTaskSet = Sets.newHashSet();
  private AlertManager alertManager;

  public TaskManager(final Configuration configuration, final Crontab crontab) {
    updateConfiguration(configuration, crontab);
    alertManager = new AlertManager(configuration);
  }

  /**
   * The main "work" routine in Omicron
   * <p/>
   * Blocks and wakes each second to check for and load updates from
   * the crontab file, or to launch tasks that are scheduled to execute
   * at the start of the current calendar minute
   *
   * @throws InterruptedException
   */
  public void run() throws InterruptedException {

    try {

      final long taskEvaluationStartMs = DateTime.now().getMillis();

      int executeCount = 0;

      for (final ScheduledTask scheduledTask : scheduledTaskSet) {

        if (scheduledTask.run()) {
          executeCount++;
        }

      }
      if (executeCount > 0) {
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
      // The only acceptable expected end to task management is by being killed as a process
      // or via a bad conf file
      error("Task evaluation exception\n{0}", Throwables.getStackTraceAsString(e));
    }
  }


  public void updateConfiguration(final Configuration configuration, final Crontab crontab) {
    this.alertManager.updateConfiguration(configuration);

    final HashSet<ScheduledTask> result = Sets.newHashSet();

    final HashSet<ScheduledTask> scheduledTaskUpdates = Sets.newHashSet();

    for (final CrontabExpression crontabExpression : crontab.getCrontabExpressions()) {

      // If there are overrides in the crontab for this expression, get them and apply them
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


}
