package com.zulily.omicron.alert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.TreeBasedTable;
import com.zulily.omicron.scheduling.ScheduledTask;
import com.zulily.omicron.sla.Policy;
import com.zulily.omicron.sla.TimeSinceLastSuccess;

import java.util.List;

public class AlertManager {
  private TreeBasedTable<ScheduledTask, String, Alert> activeAlerts = TreeBasedTable.create();
  private List<Alert> pendingAlerts = Lists.newArrayList();
  private final ImmutableList<Policy> slaPolicies = ImmutableList.of((Policy) new TimeSinceLastSuccess());

  public void evaluateSLAs(final ScheduledTask scheduledTask) {

    // Delete alerts for inactive tasks
    if (!scheduledTask.isActive()) {

      if (activeAlerts.containsRow(scheduledTask)) {
        activeAlerts.rowKeySet().remove(scheduledTask);
      }

      return;
    }

    List<Alert> newAlerts = Lists.newArrayList();

    for (Policy slaPolicy : slaPolicies) {

      final Alert alert = slaPolicy.evaluate(scheduledTask);

      if (alert != null) {
        newAlerts.add(alert);
      }

    }

    for (final Alert newAlert : newAlerts) {
      Alert existingPolicyAlert = activeAlerts.get(scheduledTask, newAlert.getPolicyName());

      if (existingPolicyAlert == null) {

        if (newAlert.isFailed()) {
          activeAlerts.put(scheduledTask, newAlert.getPolicyName(), newAlert);
          pendingAlerts.add(newAlert);
        }

        continue;
      }

      if (existingPolicyAlert.isFailed()) {

        if (newAlert.isFailed()) {
          newAlert.setLastAlertTimestamp(existingPolicyAlert.getLastAlertTimestamp());

          activeAlerts.put(scheduledTask, newAlert.getPolicyName(), newAlert);

        } else {

          activeAlerts.remove(scheduledTask, newAlert.getPolicyName());

          pendingAlerts.add(newAlert);
        }
      }


    }


  }
}
