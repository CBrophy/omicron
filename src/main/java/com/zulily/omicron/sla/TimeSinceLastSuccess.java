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
package com.zulily.omicron.sla;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.zulily.omicron.alert.Alert;
import com.zulily.omicron.alert.AlertLogEntry;
import com.zulily.omicron.alert.AlertStatus;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.scheduling.Job;
import com.zulily.omicron.scheduling.TaskLogEntry;
import com.zulily.omicron.scheduling.TaskStatus;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * SLA {@link com.zulily.omicron.sla.Policy} that generates alerts based on how long it's been
 * since a {@link Job} has seen a successful return code
 */
public final class TimeSinceLastSuccess extends Policy {

  private final static ImmutableSet<TaskStatus> STATUS_FILTER = ImmutableSet.of(TaskStatus.Complete, TaskStatus.Error, TaskStatus.FailedStart, TaskStatus.Started);
  private final static String NAME = "Time_Since_Success";

  /**
   * See {@link com.zulily.omicron.sla.Policy} class for function details
   *
   * @param job The task to be evaluated
   * @return Either a success or fail alert, or null if an evaluation can't be made
   */

  @Override
  public boolean isDisabled(final Job job) {
    // Per config comment, -1 indicates disabled alert for this policy
    return job.getConfiguration().getInt(ConfigKey.SLAMinutesSinceSuccess) == -1;
  }

  @Override
  protected Alert generateAlert(final Job job) {
    // Alert body looks like:
    //
    // SUCCESS: Time_Since_Success-> last success at 20141230 00:10 America/Los_Angeles (2 minutes ago)\nThreshold set to 20
    // FAILED: Time_Since_Success-> last success at 20141230 00:10 America/Los_Angeles (30 minutes ago)\nThreshold set to 20
    // FAILED: Time_Since_Success-> never successfully run. First attempted execution at 20141230 00:10 America/Los_Angeles (30 minutes ago)\nThreshold set to 20

    // The task has never been evaluated to run because it's new or it's not considered to be runnable to begin with
    // We cannot logically evaluate this alert
    if (!job.isRunnable() || !job.isActive() || !job.hasLogEntries()) {
      return createNotApplicableAlert(job);
    }

    final ImmutableSortedSet<TaskLogEntry> logView = job.filterLog(STATUS_FILTER);

    // Just succeed if the last log status is complete
    // This also avoids false alerts during schedule gaps
    if (logView.last().getTaskStatus() == TaskStatus.Complete) {
      return createAlert(job, logView.last(), AlertStatus.Success);
    }

    // Avoid spamming alerts during gaps in the schedule
    if (alertedOnceSinceLastActive(logView.last().getTimestamp(), job.getJobId())) {
      return createNotApplicableAlert(job);
    }

    // The last status is either error or failed start
    // so find that last time there was a complete, if any
    final Optional<TaskLogEntry> latestComplete = logView
      .descendingSet()
      .stream()
      .filter(entry -> entry.getTaskStatus() == TaskStatus.Complete)
      .findFirst();

    // If we've seen at least one success in recent history and a task is running,
    // do not alert until a final status is achieved to avoid noise before potential recovery
    if (logView.last().getTaskStatus() == TaskStatus.Started && latestComplete.isPresent()) {
      return createNotApplicableAlert(job);
    }

    final int minutesBetweenSuccessThreshold = job.getConfiguration().getInt(ConfigKey.SLAMinutesSinceSuccess);

    final long currentTimestamp = DateTime.now().getMillis();

    final TaskLogEntry baselineTaskLogEntry = latestComplete.isPresent() ? latestComplete.get() : logView.first();

    final long minutesIncomplete = TimeUnit.MILLISECONDS.toMinutes(currentTimestamp - baselineTaskLogEntry.getTimestamp());

    if (minutesIncomplete <= minutesBetweenSuccessThreshold) {
      return createAlert(job, baselineTaskLogEntry, AlertStatus.Success);
    } else {
      return createAlert(job, baselineTaskLogEntry, AlertStatus.Failure);
    }

  }

  private boolean alertedOnceSinceLastActive(final long lastActivityTimestamp, final long jobId) {
    AlertLogEntry alertLogEntry = getLastAlertLog().get(jobId);

    return alertLogEntry == null || (alertLogEntry.getStatus() == AlertStatus.Failure && alertLogEntry.getTimestamp() > lastActivityTimestamp);
  }

  private Alert createAlert(final Job job, final TaskLogEntry baselineTaskLogEntry, final AlertStatus alertStatus) {

    checkArgument(alertStatus == AlertStatus.Success || alertStatus == AlertStatus.Failure, "Alert status must be either success or failure");

    StringBuilder messageBuilder = new StringBuilder(alertStatus == AlertStatus.Success ? "SUCCESS: " : "FAILURE: ").append(NAME).append(" -> ");

    messageBuilder = messageBuilder.append(baselineTaskLogEntry.getTaskStatus() == TaskStatus.Complete ? " last complete run was at " : " no successful runs. Scheduled since ");
    messageBuilder = messageBuilder.append((new LocalDateTime(baselineTaskLogEntry.getTimestamp(), job.getConfiguration().getChronology())).toString("yyyyMMdd HH:mm"));
    messageBuilder = messageBuilder.append(" ").append(job.getConfiguration().getChronology().getZone().toString());
    messageBuilder = messageBuilder.append(" (").append(TimeUnit.MILLISECONDS.toMinutes(DateTime.now().getMillis() - baselineTaskLogEntry.getTimestamp())).append(" minutes ago)");

    messageBuilder = messageBuilder.append("\nThreshold set to ").append(job.getConfiguration().getInt(ConfigKey.SLAMinutesSinceSuccess));

    return new Alert(messageBuilder.toString(), job, alertStatus);
  }

}
