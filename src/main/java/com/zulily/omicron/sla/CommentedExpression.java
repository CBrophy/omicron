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

public class CommentedExpression extends Policy {

  private final static String NAME = "Commented_Expression";

  @Override
  public boolean isDisabled(final Job job) {
    // Per config comment, -1 indicates disabled alert for this policy
    return job.getConfiguration().getInt(ConfigKey.SLACommentedExpressionAlertDelayMinutes) == -1;
  }

  @Override
  protected Alert generateAlert(final Job job) {
    final int commentedAlertIntervalMinutes = job.getConfiguration().getInt(ConfigKey.SLACommentedExpressionAlertDelayMinutes);

    final CrontabExpression crontabExpression = job.getCrontabExpression();

    final long crontabReadTimestamp = crontabExpression.getTimestamp();

    final long currentTimestamp = DateTime.now().getMillis();

    final int minutesCommented = (int) TimeUnit.MILLISECONDS.toMinutes(currentTimestamp - crontabReadTimestamp);

    final AlertStatus alertStatus = crontabExpression.isCommented() && minutesCommented > commentedAlertIntervalMinutes ? AlertStatus.Failure : AlertStatus.Success;

    // Alert body looks like:
    //
    // SUCCESS: Commented_Expression-> expression uncommented and scheduled to run
    // FAILED: Commented_Expression-> row is commented and disabled (commented out for 40 minutes; threshold set to 20)

    StringBuilder messageBuilder = new StringBuilder(NAME).append("->");

    if (alertStatus == AlertStatus.Failure) {
      messageBuilder = messageBuilder
        .append(" row is commented and disabled (commented out for ")
        .append(minutesCommented)
        .append(" minutes; threshold set to ")
        .append(commentedAlertIntervalMinutes)
        .append(")");

    } else {
      messageBuilder = messageBuilder.append(" expression uncommented and scheduled to run");
    }

    return new Alert(
      messageBuilder.toString(),
      job,
      alertStatus
    );
  }
}
