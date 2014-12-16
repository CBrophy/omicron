package com.zulily.omicron.sla;

import com.zulily.omicron.Configuration;
import com.zulily.omicron.ScheduledTask;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.info;

public class TimeSinceLastSuccess implements Policy {
  private final Configuration configuration;

  public TimeSinceLastSuccess(final Configuration configuration) {
    this.configuration = checkNotNull(configuration, "configuration");
  }

  @Override
  public boolean evaluate(ScheduledTask scheduledTask) {
    final long baseTimestamp = scheduledTask.getLastSuccessTimestamp() > 0L ? scheduledTask.getLastSuccessTimestamp() : scheduledTask.getFirstExecutionTimestamp();

    final boolean result = DateTime.now().getMillis() - baseTimestamp <= configuration.getSlaMinMillisSinceSuccess();

    if (!result) {
      info("{0} failed {1}: last success at {2}", scheduledTask.toString(), getName(), (new LocalDateTime(scheduledTask.getLastSuccessTimestamp(), configuration.getChronology())).toString());
    }

    return result;
  }

  @Override
  public boolean enabled() {
    return configuration.getSlaMinMillisSinceSuccess() > 0L;
  }

  @Override
  public String getName() {
    return "Time_Since_Success";
  }
}
