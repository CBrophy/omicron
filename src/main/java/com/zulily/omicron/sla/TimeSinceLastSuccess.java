package com.zulily.omicron.sla;

import com.zulily.omicron.Configuration;
import com.zulily.omicron.ScheduledTask;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.info;

public class TimeSinceLastSuccess implements Policy {
  private final Configuration configuration;

  public TimeSinceLastSuccess(final Configuration configuration){
    this.configuration = checkNotNull(configuration, "configuration");
  }

  @Override
  public boolean evaluate(ScheduledTask scheduledTask) {
    boolean result = DateTime.now().getMillis() - scheduledTask.getLastSuccessTimestamp() <= configuration.getSlaMinMillisSinceSuccess();

    if(!result) {
      info(String.format("%s failed %s: last success at %s", scheduledTask.toString(), getName(), (new LocalDateTime(scheduledTask.getLastSuccessTimestamp(), configuration.getChronology())).toString("yyyyMMdd HH:mm:ss")));
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
