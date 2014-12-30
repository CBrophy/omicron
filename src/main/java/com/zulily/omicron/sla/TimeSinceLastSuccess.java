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

import com.zulily.omicron.Utils;
import com.zulily.omicron.alert.Alert;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.crontab.CrontabExpression;
import com.zulily.omicron.scheduling.ScheduledTask;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.concurrent.TimeUnit;

/**
 * SLA {@link com.zulily.omicron.sla.Policy} that generates alerts based on how long it's been
 * since a {@link com.zulily.omicron.scheduling.ScheduledTask} has seen a successful return code
 */
public final class TimeSinceLastSuccess implements Policy {

  /**
   * See {@link com.zulily.omicron.sla.Policy} class for function details
   *
   * @param scheduledTask The task to be evaluated
   * @return Either a success or fail alert, or null if an evaluation can't be made
   */
  @Override
  public Alert evaluate(final ScheduledTask scheduledTask) {

    // The task has never been evaluated to run (it's new) - cannot alert yet
    // so just return null
    if (scheduledTask.getFirstExecutionTimestamp() == Utils.DEFAULT_TIMESTAMP) {
      return null;
    }

    // The last activity timestamp will return the last success timestamp, or the very
    // first execution timestamp if the task has never had a successful run
    final long lastActiveTimestamp = getLastActiveTimestamp(scheduledTask);

    final int minutesBetweenSuccessThreshold = scheduledTask.getConfiguration().getInt(ConfigKey.SLAMinutesSinceSuccess);

    final long currentTimestamp = DateTime.now().getMillis();

    final CrontabExpression crontabExpression = scheduledTask.getCrontabExpression();

    final Chronology chronology = scheduledTask.getConfiguration().getChronology();

    final long minutesSinceLastActivity = TimeUnit.MILLISECONDS.toMinutes(currentTimestamp - lastActiveTimestamp);

    final boolean failed = currentTimestamp - lastActiveTimestamp > TimeUnit.MINUTES.toMillis(minutesBetweenSuccessThreshold);

    // Alert body looks like:
    //
    // SUCCESS: Time_Since_Success-> last success at 20141230 00:10 America/Los_Angeles (2 minutes ago; threshold set to 20)
    // FAILED: Time_Since_Success-> last success at 20141230 00:10 America/Los_Angeles (30 minutes ago; threshold set to 20)
    // FAILED: Time_Since_Success-> never successfully run. First attempted execution at 20141230 00:10 America/Los_Angeles (30 minutes ago; threshold set to 20)

    StringBuilder messageBuilder = new StringBuilder(getName()).append("->");

    if (scheduledTask.getTotalSuccessCount() == 0 && failed) {
      messageBuilder = messageBuilder.append(" never successfully run. First attempted execution at ");
    } else {
      messageBuilder = messageBuilder.append(" last success was at ");
    }

    messageBuilder = messageBuilder.append((new LocalDateTime(lastActiveTimestamp, chronology)).toString("yyyyMMdd HH:mm"));
    messageBuilder = messageBuilder.append(" ").append(chronology.getZone().toString());
    messageBuilder = messageBuilder.append(" (").append(minutesSinceLastActivity).append(" minutes ago; threshold set to ").append(minutesBetweenSuccessThreshold).append(")");

    return new Alert(
      getName(),
      messageBuilder.toString(),
      crontabExpression.getLineNumber(),
      crontabExpression.getRawExpression(),
      failed
    );

  }

  private long getLastActiveTimestamp(final ScheduledTask scheduledTask) {
    return scheduledTask.getTotalSuccessCount() > 0 // if there is a last success at all
      ? scheduledTask.getLastSuccessTimestamp() // use the timestamp
      : scheduledTask.getFirstExecutionTimestamp(); // otherwise, use the first execution timestamp for a baseline
  }

  @Override
  public String getName() {
    return "Time_Since_Success";
  }
}
