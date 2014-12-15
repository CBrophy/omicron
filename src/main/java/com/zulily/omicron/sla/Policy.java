package com.zulily.omicron.sla;

import com.zulily.omicron.ScheduledTask;

public interface Policy {
  boolean evaluate(final ScheduledTask scheduledTask);
  boolean enabled();
  String getName();
}
