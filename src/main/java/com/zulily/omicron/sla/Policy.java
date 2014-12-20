package com.zulily.omicron.sla;

import com.zulily.omicron.alert.Alert;
import com.zulily.omicron.scheduling.ScheduledTask;

/**
 * A Policy represents a set of rules by which a decision is made to send an alert or not
 *
 * A policy should return either a success or failed alert. The AlertManager class determines
 * what to ultimately do with the results
 */
public interface Policy {

  /**
   * Evaluate the statistics or properties of the provided ScheduledTask instance
   * and produce either a successful or failed alert instance.
   *
   * @param scheduledTask The task to be evaluated
   * @return Either a success or failed alert. In the case where a binary Alert state cannot be determined, return null.
   */
  Alert evaluate(final ScheduledTask scheduledTask);

  String getName();
}
