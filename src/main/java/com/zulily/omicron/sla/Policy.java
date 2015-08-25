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

import com.google.common.collect.Sets;
import com.zulily.omicron.alert.Alert;
import com.zulily.omicron.alert.AlertLogEntry;
import com.zulily.omicron.alert.AlertStatus;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.TimeInterval;
import com.zulily.omicron.scheduling.Job;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * A Policy represents a set of rules by which a decision is made to send an alert or not
 * <p>
 * A policy should return either a success or failed alert. The AlertManager class determines
 * what to ultimately do with the results
 */
public abstract class Policy {

  private final TreeMap<Long, AlertLogEntry> lastAlertLog = new TreeMap<>();

  public abstract boolean isDisabled(final Job job);

  protected abstract Alert generateAlert(final Job job);

  public List<Alert> evaluate(final Iterable<Job> jobs) {

    final List<Alert> result = new ArrayList<>();

    final Set<Long> activeJobIds = new HashSet<>();

    for (Job job : jobs) {

      if (job.isActive() && !isDisabled(job) && !isDowntime(job)) {

        activeJobIds.add(job.getJobId());

        final Alert alert = generateAlert(job);

        if (alert.getAlertStatus() == AlertStatus.NotApplicable) {
          continue;
        }

        AlertLogEntry logEntry = lastAlertLog.get(job.getJobId());

        if (logEntry != null) {

          // Do not alert multiple times for success
          if (alert.getAlertStatus() == AlertStatus.Success && logEntry.getStatus() == AlertStatus.Success) {
            continue;
          }

          // Do not repeat alerts within the threshold
          if (alert.getAlertStatus() == AlertStatus.Failure && delayRepeat(logEntry, job)) {
            continue;
          }

        } else if (alert.getAlertStatus() != AlertStatus.Failure) {
          continue;
        }

        lastAlertLog.put(job.getJobId(), new AlertLogEntry(job.getJobId(), alert.getAlertStatus()));

        result.add(alert);

      }

    }

    // Remove alerts for inactive jobs
    final Sets.SetView<Long> oldAlerts = Sets.difference(lastAlertLog.keySet(), activeJobIds);

    for (Long oldJobId : oldAlerts) {

      lastAlertLog.remove(oldJobId);

    }

    return result;
  }

  private boolean delayRepeat(final AlertLogEntry alertLogEntry, final Job job) {

    return DateTime.now().getMillis() - alertLogEntry.getTimestamp() <= TimeUnit.MINUTES.toMillis(
      job
        .getConfiguration()
        .getInt(ConfigKey.AlertMinutesDelayRepeat));

  }

  protected TreeMap<Long, AlertLogEntry> getLastAlertLog() {
    return lastAlertLog;
  }

  protected Alert createNotApplicableAlert(final Job job) {
    return new Alert(
      "",
      job,
      AlertStatus.NotApplicable
    );
  }

  private boolean isDowntime(final Job job) {
    TimeInterval timeInterval = job.getConfiguration().getTimeInterval(ConfigKey.AlertDowntime);

    if (timeInterval == null) {
      return false;
    }

    Interval interval = timeInterval.asInterval(job.getConfiguration().getChronology());

    return interval.contains(DateTime.now(job.getConfiguration().getChronology()));
  }

}
