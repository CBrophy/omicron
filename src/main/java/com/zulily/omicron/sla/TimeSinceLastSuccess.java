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

import com.zulily.omicron.alert.Alert;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;
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
public class TimeSinceLastSuccess implements Policy {

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
    if (scheduledTask.getFirstExecutionTimestamp() == Configuration.DEFAULT_TIMESTAMP) {
      return null;
    }

    final long baseTimestamp = scheduledTask.getLastSuccessTimestamp() > Configuration.DEFAULT_TIMESTAMP // if there is a last success timestamp
      ? scheduledTask.getLastSuccessTimestamp() // use it
      : scheduledTask.getFirstExecutionTimestamp(); // otherwise, use the first execution timestamp because the task has never succeeded

    final int minutesBetweenSuccess = scheduledTask.getConfiguration().getInt(ConfigKey.SLAMinutesSinceSuccess);

    final boolean failed = DateTime.now().getMillis() - baseTimestamp > TimeUnit.MINUTES.toMillis(minutesBetweenSuccess);

    return createAlert(scheduledTask, failed, baseTimestamp);
  }

  private Alert createAlert(final ScheduledTask scheduledTask, final boolean failed, long baseTimestamp) {
    final CrontabExpression crontabExpression = scheduledTask.getCrontabExpression();
    final Chronology chronology = scheduledTask.getConfiguration().getChronology();

    // Alerts are displayed as grouped by the crontab command string, so there is
    // no need to print it out in the alert message
    return new Alert(
      getName(),
      String.format("%s %s: last success at %s", failed ? "failed" : "succeeded", getName(), (new LocalDateTime(baseTimestamp, chronology)).toString("yyyyMMdd HH:mm")),
      crontabExpression.getLineNumber(),
      crontabExpression.getRawExpression(),
      failed
    );
  }

  @Override
  public String getName() {
    return "Time_Since_Success";
  }
}
