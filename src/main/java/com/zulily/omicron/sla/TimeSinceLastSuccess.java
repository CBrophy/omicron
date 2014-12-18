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


public class TimeSinceLastSuccess implements Policy {

  @Override
  public Alert evaluate(final ScheduledTask scheduledTask) {
    // The task has never been evaluated to run - cannot alert yet
    if (scheduledTask.getFirstExecutionTimestamp() == Configuration.DEFAULT_TIMESTAMP) {
      return null;
    }

    final long baseTimestamp = scheduledTask.getLastSuccessTimestamp() > Configuration.DEFAULT_TIMESTAMP // if there is a last success timestamp
      ? scheduledTask.getLastSuccessTimestamp() // use it
      : scheduledTask.getFirstExecutionTimestamp(); // otherwise, use the first execution timestamp because the task has never succeeded

    final int millisecondsBetweenSuccess = scheduledTask.getConfiguration().getInt(ConfigKey.SLAMinutesSinceSuccess);

    final boolean failed = DateTime.now().getMillis() - baseTimestamp > TimeUnit.MINUTES.toMillis(millisecondsBetweenSuccess);

    return createAlert(scheduledTask, failed, baseTimestamp);
  }

  private Alert createAlert(final ScheduledTask scheduledTask, final boolean failed, long baseTimestamp) {
    final CrontabExpression crontabExpression = scheduledTask.getCrontabExpression();
    final Chronology chronology = scheduledTask.getConfiguration().getChronology();

    return new Alert(
      getName(),
      String.format("%s %s: last success at %s", failed ? "failed" : "succeeded", getName(), (new LocalDateTime(baseTimestamp, chronology)).toString()),
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
