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
import com.zulily.omicron.crontab.CronVariable;
import com.zulily.omicron.crontab.Crontab;
import com.zulily.omicron.crontab.CrontabExpression;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;

/**
 * The container class for scheduled tasks, and the logical engine for launching tasks
 * and triggering alert SLA evaluation.
 */
public final class JobManager {
  private final ArrayList<Job> retiredJobs = Lists.newArrayList();
  private HashSet<Job> jobSet = Sets.newHashSet();
  private AlertManager alertManager;

  public JobManager(final Configuration configuration, final Crontab crontab) {
    checkNotNull(configuration, "configuration");
    checkNotNull(crontab, "crontab");

    alertManager = new AlertManager(configuration);

    updateConfiguration(configuration, crontab);

  }

  /**
   * The main "work" routine in taskmanager
   * <p/>
   * Loops through the task list and attempts to run each
   * <p/>
   * After tasks are run, the alert manager is triggered
   * to evaluate the subsequent state of the tasks and send
   * alerts accordingly
   */
  public void run() {


    final long taskEvaluationStartMs = DateTime.now().getMillis();

    int executeCount = 0;

    for (final Job job : jobSet) {

      try {

        if (job.run()) {
          executeCount++;
        }

      } catch (Exception e) {
        // An individual task failure should never block all other tasks from executing, so output any exceptions and continue
        error("Task evaluation exception on task: {0}\n{1}", job.toString(), Throwables.getStackTraceAsString(e));
      }

    }

    if (executeCount > 0) {
      info("Task evaluation took {0} ms: running {1} task(s)", String.valueOf(DateTime.now().getMillis() - taskEvaluationStartMs), String.valueOf(executeCount));
    }

    try {
      // Tasks that have been removed from execution due to a crontab
      // update will remain referenced until all processes associated with the
      // old task return
      retireOldTasks();

      // Perform the alert evaluation outside of the task launching loop
      // to avoid delaying task launch and to skip evaluation
      // against retired tasks
      alertManager.sendAlerts(jobSet);

    } catch (Exception e) {
      // This function should not throw exceptions that cause the outer timed loop to break
      error("Exception while retiring old tasks or sending notifications\n{0}", Throwables.getStackTraceAsString(e));
    }
  }


  /**
   * Updates the scheduled tasks and alert manager with any changes from the config or crontab
   *
   * @param configuration The more current global configuration instance
   * @param crontab       The more current crontab
   */
  public void updateConfiguration(final Configuration configuration, final Crontab crontab) {
    checkNotNull(configuration, "configuration");
    checkNotNull(crontab, "crontab");
    checkNotNull(alertManager, "alertManager");

    this.alertManager.updateConfiguration(configuration);

    final HashSet<Job> result = Sets.newHashSet();

    final HashSet<Job> jobUpdates = Sets.newHashSet();

    for (final CrontabExpression crontabExpression : crontab.getCrontabExpressions()) {

      // If there are overrides in the crontab for this expression, get them and apply them
      final Configuration configurationOverride = crontab.getConfigurationOverrides().get(crontabExpression.getLineNumber());

      final Job job = new Job(
        crontabExpression,
        substituteVariables(crontabExpression.getCommand(), crontab.getVariables()),
        configurationOverride == null ? configuration : configurationOverride);

      jobUpdates.add(job);
    }

    // This is a view containing old scheduled tasks that have been removed or
    // reconfigured
    final Sets.SetView<Job> oldJobs = Sets.difference(jobSet, jobUpdates);

    info("CRON UPDATE: {0} tasks no longer scheduled or out of date", String.valueOf(oldJobs.size()));

    // This is a view of scheduled tasks that will not be updated by the cron reload
    final Sets.SetView<Job> existingJobs = Sets.intersection(jobSet, jobUpdates);

    info("CRON UPDATE: {0} tasks unchanged", String.valueOf(existingJobs.size()));

    // This is a view of scheduled tasks that are new or have been changed
    final Sets.SetView<Job> newJobs = Sets.difference(jobUpdates, jobSet);

    info("CRON UPDATE: {0} tasks are new or updated", String.valueOf(newJobs.size()));

    // Add all new tasks
    // keep references to old tasks that are still running
    // and transfer instances that haven't changed
    result.addAll(newJobs);

    for (final Job job : jobSet) {

      if (oldJobs.contains(job) && job.isRunning()) {

        job.setActive(false);
        result.add(job);

        retiredJobs.add(job);
      }

      if (existingJobs.contains(job)) {

        if (!job.isActive()) {
          // Did someone re-add a task that was running and then removed?
          // For whatever reason, it's now set to run again so just re-activate the instance
          info("CRON UPDATE: Reactivating {0}", job.toString());
          job.setActive(true);
        }

        result.add(job);
      }
    }

    this.jobSet = result;
  }

  private void retireOldTasks() {
    final int retiredTaskCount = retiredJobs.size() - 1;

    for (int index = retiredTaskCount; index >= 0; index--) {
      final Job retiredTask = retiredJobs.get(index);

      if (!retiredTask.isRunning()) {
        info("Retiring inactive task: {0}", retiredTask.toString());
        retiredJobs.remove(index);
        jobSet.remove(retiredTask);
      }
    }
  }

  private static String substituteVariables(final String line, final List<CronVariable> variableList) {
    String substituted = line;

    for (final CronVariable cronVariable : variableList) {
      substituted = cronVariable.applySubstitution(substituted);
    }

    return substituted;
  }


}
