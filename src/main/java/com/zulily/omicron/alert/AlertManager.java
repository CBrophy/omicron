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
package com.zulily.omicron.alert;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.zulily.omicron.Utils;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;
import com.zulily.omicron.scheduling.Job;
import com.zulily.omicron.sla.CommentedExpression;
import com.zulily.omicron.sla.MalformedExpression;
import com.zulily.omicron.sla.Policy;
import com.zulily.omicron.sla.TimeSinceLastSuccess;

import javax.mail.internet.AddressException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;


/**
 * The alert manager validates the executing jobs against a list of known SLAs and
 * will send email notifications when failures/successes are detected
 * <p>
 * See: {@link com.zulily.omicron.sla.Policy}, {@link com.zulily.omicron.alert.Alert}
 */
public final class AlertManager {

  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private final ImmutableList<Policy> slaPolicies = ImmutableList.of(
    (Policy) new TimeSinceLastSuccess(),
    (Policy) new MalformedExpression(),
    (Policy) new CommentedExpression());

  // Email sender can be updated by a live config change, while the pending policy lists won't change
  private EmailSender email;

  /**
   * Constructor
   *
   * @param configuration The loaded global configuration
   */
  public AlertManager(final Configuration configuration) {
    checkNotNull(configuration, "email");

    updateConfiguration(configuration);
  }

  /**
   * Initializes a new EmailSender with updated parameters
   *
   * @param configuration The configuration to use for the EmailSender
   */
  public void updateConfiguration(final Configuration configuration) {
    try {

      this.email = EmailSender.from(configuration.getString(ConfigKey.AlertEmailAddressFrom))
        .to(Utils.COMMA_SPLITTER.split(configuration.getString(ConfigKey.AlertEmailAddressTo)))
        .withSMTPServer(configuration.getString(ConfigKey.AlertEmailSmtpHost), Integer.parseInt(configuration.getString(ConfigKey.AlertEmailSmtpPort)))
        .build();

    } catch (AddressException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Evaluates the passed in collection of {@link Job} instances
   * against the list of known {@link com.zulily.omicron.sla.Policy} implementations
   *
   * @param scheduledTasks The scheduled tasks to evaluate
   */
  public void sendAlerts(final Iterable<Job> scheduledTasks) {

    // Group all alerts into a single email try to mitigate getting mobbed by many single alert messages

    final List<Alert> alertsToSend = new ArrayList<>();

    for (Policy slaPolicy : slaPolicies) {

      alertsToSend.addAll(slaPolicy.evaluate(scheduledTasks));

    }

    if (!alertsToSend.isEmpty()) {
      this.threadPool.submit(new SendEmailRunnable(alertsToSend, this.email));
    }

  }


  /**
   * This runnable does the work of building the email body and sending it out to the SMTP server
   */
  private static class SendEmailRunnable implements Runnable {
    private final List<Alert> alerts;
    private final com.zulily.omicron.alert.EmailSender email;

    SendEmailRunnable(final List<Alert> alerts, final com.zulily.omicron.alert.EmailSender email) {
      this.alerts = checkNotNull(alerts, "alerts");
      this.email = checkNotNull(email, "email");
    }

    @Override
    public void run() {
      if (alerts.isEmpty()) {
        return;
      }

      try {
        // Subject ->
        // [OMICRON ALERT: <hostname>] failures: # successes: #

        // Body ->
        // Alerts are listed in order of crontab command and alert timestamp
        //
        // <crontab line>
        //    FAIL/SUCCESS: <alert message>
        //    FAIL/SUCCESS: <alert message>
        //
        // ... repeat for each crontab line with alert(s)
        //
        // Sincerely,
        // Omicron <3

        StringBuilder bodyBuilder = new StringBuilder("Alerts are listed in order of crontab command and alert timestamp\n\n");

        StringBuilder subjectBuilder = new StringBuilder("[OMICRON ALERT: ").append(Utils.getHostName()).append("]");

        int failedCount = 0;
        int successCount = 0;

        for (final Alert alert : alerts) {

          bodyBuilder = bodyBuilder.append(alert.getJob().getCrontabExpression()).append("\n\n");

          if (alert.getAlertStatus() == AlertStatus.Failure) {
            failedCount++;

            bodyBuilder = bodyBuilder.append("\tFAIL: ");
          } else {
            successCount++;

            bodyBuilder = bodyBuilder.append("\tSUCCESS: ");
          }

          bodyBuilder = bodyBuilder.append(alert.getMessage()).append('\n');

        }

        bodyBuilder = bodyBuilder.append('\n');

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
