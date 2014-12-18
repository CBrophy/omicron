package com.zulily.omicron.alert;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.zulily.omicron.Utils;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.scheduling.ScheduledTask;
import com.zulily.omicron.sla.Policy;
import com.zulily.omicron.sla.TimeSinceLastSuccess;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;


public class AlertManager {
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private final ImmutableList<Policy> slaPolicies = ImmutableList.of((Policy) new TimeSinceLastSuccess());

  private final Email email;

  public AlertManager(final Email email) {
    this.email = checkNotNull(email, "email");
  }

  private void evaluateSLAs(final ScheduledTask scheduledTask) {

    // Ignore alerts on in active tasks
    if (!scheduledTask.isActive()) {
      return;
    }

    for (Policy slaPolicy : slaPolicies) {

      final Alert newAlert = slaPolicy.evaluate(scheduledTask);

      if (newAlert != null) {

        final Alert existingAlert = scheduledTask.getPolicyAlerts().get(slaPolicy.getName());

        if (isSilentSuccess(existingAlert, newAlert)) {
          // We don't care about success alerts unless there is an existing
          // failure, indicating a recovery state to notify against

          continue;
        }

        if (isFailureUpdate(existingAlert, newAlert)) {
          // The concept is that the policy alert message will may have changed (i.e. new failure durations/state, etc)
          // so overwrite the existing alert with the most recent outcome to get any updates
          //
          // However, we *do* need the last alert timestamp from the old alert instance
          // to prevent spamming alerts until the alert delay timeout is over

          newAlert.setLastAlertTimestamp(existingAlert.getLastAlertTimestamp());

        }

        // the remaining states are
        // recovery = existingAlert.isFailed and !newAlert.isFailed -> just put newAlert
        // newFailure = existingAlert is null && newAlert.isFailed -> just put newAlert
        //
        // in either case, there is no longer a need to test the existing alert
        // and it doesn't matter what the failure state of the new alert might be

        scheduledTask.getPolicyAlerts().put(slaPolicy.getName(), newAlert);

      }

    }

  }

  private static boolean isSilentSuccess(final Alert existingAlert, final Alert newAlert) {
    return existingAlert == null && !newAlert.isFailed();
  }


  private static boolean isFailureUpdate(final Alert existingAlert, final Alert newAlert) {
    return existingAlert != null && existingAlert.isFailed() && newAlert.isFailed();
  }

  public void sendAlerts(final Iterable<ScheduledTask> scheduledTasks) {

    // Group all alerts into a single email try to mitigate getting mobbed by many single alert messages

    final TreeMultimap<String, Alert> alertsToSend = TreeMultimap.create();

    for (final ScheduledTask scheduledTask : scheduledTasks) {

      evaluateSLAs(scheduledTask);

      if (scheduledTask.getPolicyAlerts().isEmpty()) {
        continue;
      }

      if (!scheduledTask.getConfiguration().getBoolean(ConfigKey.AlertEmailEnabled)) {

        warn("{0} Unsent policy alerts cleared due to disabled email alerting in config for: {1}", String.valueOf(scheduledTask.getPolicyAlerts().size()), scheduledTask.toString());

        scheduledTask.getPolicyAlerts().clear();

        continue;

      }

      final int alertMinutesDelayRepeat = scheduledTask.getConfiguration().getInt(ConfigKey.AlertMinutesDelayRepeat);

      // Some alerts aren't meant to be repeated (recovery) so remove them afterwards
      final HashSet<String> policyAlertsToRemove = Sets.newHashSet();

      for (final Alert alert : scheduledTask.getPolicyAlerts().values()) {

        if (skipAlert(alert, alertMinutesDelayRepeat)) {
          continue;
        }

        //Send recovery alerts immediately and don't repeat them
        if (!alert.isFailed()) {

          policyAlertsToRemove.add(alert.getPolicyName());

        }

        alertsToSend.put(scheduledTask.toString(), alert);

      }

      for (final String policyName : policyAlertsToRemove) {
        scheduledTask.getPolicyAlerts().remove(policyName);
      }

    }

    this.threadPool.submit(new EmailSender(alertsToSend, this.email));

    this.threadPool.shutdown();

  }

  private static boolean skipAlert(final Alert alert, final int alertMinutesDelayRepeat) {
    return alert.isFailed() && DateTime.now().getMillis() - alert.getLastAlertTimestamp() <= TimeUnit.MINUTES.toMillis(alertMinutesDelayRepeat);
  }

  private static class EmailSender implements Runnable {
    private final TreeMultimap<String, Alert> alerts;
    private final Email email;

    EmailSender(final TreeMultimap<String, Alert> alerts, final Email email) {
      this.alerts = checkNotNull(alerts, "alerts");
      this.email = checkNotNull(email, "email");
    }

    @Override
    public void run() {
      if (alerts.isEmpty()) {
        return;
      }


      try {

        StringBuilder bodyBuilder = new StringBuilder("Alerts are listed in order of crontab command and alert timestamp\n\n");

        StringBuilder subjectBuilder = new StringBuilder("[OMICRON ALERT: ").append(Utils.getHostName()).append("]");

        int failedCount = 0;
        int successCount = 0;

        for (final String commandLine : alerts.keySet()) {

          bodyBuilder = bodyBuilder.append(commandLine).append("\n\n");

          for (final Alert alert : alerts.get(commandLine)) {

            if (alert.isFailed()) {
              failedCount++;

              bodyBuilder = bodyBuilder.append("FAIL: ");
            } else {
              successCount++;

              bodyBuilder = bodyBuilder.append("SUCCESS: ");
            }

            bodyBuilder = bodyBuilder.append(alert.getMessage()).append('\n');

          }

          bodyBuilder = bodyBuilder.append('\n');

        }

        if (failedCount > 0) {
          subjectBuilder.append(" failures: ").append(failedCount);
        }

        if (successCount > 0) {
          subjectBuilder.append(" successes: ").append(successCount);
        }

        bodyBuilder = bodyBuilder.append("Sincerely,\nOmicron <3");

        info("Sending alert email with {0} successes and {1} failures", String.valueOf(successCount), String.valueOf(failedCount));

        email.send(subjectBuilder.toString(), bodyBuilder.toString());
      } catch (Exception e) {
        error("Failed to send alerts due to an exception:\n{0}", Throwables.getStackTraceAsString(e));
      }
    }
  }
}
