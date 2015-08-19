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
import com.zulily.omicron.crontab.CrontabExpression;
import com.zulily.omicron.scheduling.CronJob;
import org.joda.time.DateTime;

import java.util.concurrent.TimeUnit;

public class CommentedExpression implements Policy {

  @Override
  public Alert evaluate(final CronJob cronJob) {

    final int commentedAlertIntervalMinutes = cronJob.getConfiguration().getInt(ConfigKey.SLACommentedExpressionAlertDelayMinutes);

    final CrontabExpression crontabExpression = cronJob.getCrontabExpression();

    final long crontabReadTimestamp = crontabExpression.getTimestamp();

    final long currentTimestamp = DateTime.now().getMillis();

    final int minutesCommented = (int) TimeUnit.MILLISECONDS.toMinutes(currentTimestamp - crontabReadTimestamp);

    final boolean failed = crontabExpression.isCommented() && minutesCommented > commentedAlertIntervalMinutes;

    // Alert body looks like:
    //
    // SUCCESS: Commented_Expression-> expression uncommented and scheduled to run
    // FAILED: Commented_Expression-> row is commented and disabled (commented out for 40 minutes; threshold set to 20)


    StringBuilder messageBuilder = new StringBuilder(getName()).append("->");

    if (failed) {
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
      getName(),
      messageBuilder.toString(),
      crontabExpression.getLineNumber(),
      crontabExpression.getRawExpression(),
      failed
    );
  }

  @Override
  public String getName() {
    return "Commented_Expression";
  }

  @Override
  public boolean isDisabled(final CronJob cronJob) {
    // Per config comment, -1 indicates disabled alert for this policy
    return cronJob.getConfiguration().getInt(ConfigKey.SLACommentedExpressionAlertDelayMinutes) == -1;
  }
}
