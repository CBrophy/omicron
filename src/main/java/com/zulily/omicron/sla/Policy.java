package com.zulily.omicron.sla;

import com.zulily.omicron.alert.Alert;
import com.zulily.omicron.scheduling.ScheduledTask;

public interface Policy {
  Alert evaluate(final ScheduledTask scheduledTask);

  String getName();
}
