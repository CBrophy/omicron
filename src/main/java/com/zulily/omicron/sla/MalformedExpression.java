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
import com.zulily.omicron.alert.AlertStatus;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.crontab.CrontabExpression;
import com.zulily.omicron.scheduling.Job;
import org.joda.time.DateTime;

import java.util.concurrent.TimeUnit;

public class MalformedExpression extends Policy {

  private final static String NAME = "Malformed_Expression";

  @Override
  public boolean isDisabled(final Job job) {
    // Per config comment, -1 indicates disabled alert for this policy
    return job.getConfiguration().getInt(ConfigKey.SLAMalformedExpressionAlertDelayMinutes) == -1;
  }

  @Override
  protected Alert generateAlert(final Job job) {
    // Alert body looks like:
    //
    // SUCCESS: Malformed_Expression-> expression is valid and scheduled to run
    // FAILED: Malformed_Expression-> row is uncommented but cannot be run due to syntax error (malformed for 40 minutes; threshold set to 20)

    final int malformedAlertDelaylMinutes = job.getConfiguration().getInt(ConfigKey.SLAMalformedExpressionAlertDelayMinutes);

    final CrontabExpression crontabExpression = job.getCrontabExpression();

    final long crontabReadTimestamp = crontabExpression.getTimestamp();

    final long currentTimestamp = DateTime.now().getMillis();

    final int minutesMalformed = (int) TimeUnit.MILLISECONDS.toMinutes(currentTimestamp - crontabReadTimestamp);

    final AlertStatus alertStatus = crontabExpression.isMalformed() && minutesMalformed > malformedAlertDelaylMinutes ? AlertStatus.Failure : AlertStatus.Success;

    StringBuilder messageBuilder = new StringBuilder(alertStatus == AlertStatus.Failure ? "FAILED: " : "SUCCESS: ").append(NAME).append(" -> ");

    if (alertStatus == AlertStatus.Failure) {
      messageBuilder = messageBuilder
        .append(" row is uncommented but cannot be run due to syntax error (malformed for ")
        .append(minutesMalformed)
        .append(" minutes; threshold set to ")
        .append(malformedAlertDelaylMinutes)
        .append(")");

    } else {
      messageBuilder = messageBuilder.append(" expression is valid and scheduled to run");
    }

    return new Alert(
      messageBuilder.toString(),
      job,
      alertStatus
    );
  }
}
