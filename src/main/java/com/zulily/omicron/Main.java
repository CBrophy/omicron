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
package com.zulily.omicron;

import com.google.common.base.Throwables;
import com.zulily.omicron.conf.ConfigKey;
import com.zulily.omicron.conf.Configuration;
import com.zulily.omicron.crontab.Crontab;
import com.zulily.omicron.scheduling.TaskManager;
import org.joda.time.DateTime;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.zulily.omicron.Utils.error;
import static com.zulily.omicron.Utils.info;
import static com.zulily.omicron.Utils.warn;

public final class Main {

  public static void main(final String[] args) {

    if (args == null || (args.length > 0 && args[0].contains("?"))) {
      printHelp();
      System.exit(0);
    }

    // see doc for java.util.logging.SimpleFormatter
    // format output will look like:
    // [Tue Dec 16 10:29:07 PST 2014] INFO: <message>
    System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tc] %4$s: %5$s %n");

    try {

      Configuration configuration = new Configuration(args.length > 0 ? args[0].trim() : "/etc/omicron/omicron.conf");

      Crontab crontab = new Crontab(configuration);

      final TaskManager taskManager = new TaskManager(configuration, crontab);

      // The minute logic is intended to stay calibrated
      // with the current calendar minute.
      // Scheduled jobs should run as close to second-of-minute=0 as possible
      // while minimizing acquired execution drift over time

      long targetExecuteMinute = getMinuteMillisFromNow(1);

      //noinspection InfiniteLoopStatement
      while (true) {

        long currentExecuteMinute = getMinuteMillisFromNow(0);
        // Trigger when the execute minute comes up or is past-due
        // until then watch for crontab changes or sleep
        while (currentExecuteMinute < targetExecuteMinute) {

          if (configurationUpdated(crontab, configuration)) {

            info("Either configuration or crontab updated. Reloading task configurations.");

            configuration = configuration.reload();
            crontab = new Crontab(configuration);

            taskManager.updateConfiguration(configuration, crontab);
          }

          try {

            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }

          currentExecuteMinute = getMinuteMillisFromNow(0);

        }

        // Due to drift, the previous cycle or due to the length of
        // time it takes to read crontab/config changes,
        // we may actually pass a calendar minute without evaluation
        // of the scheduled task list
        //
        // The current implementation of crond never evaluates the current minute
        // that it detects schedule changes. For now I'm calling that
        // particular case "expected behavior"
        if (currentExecuteMinute != targetExecuteMinute) {
          warn("Scheduled tasks may have been missed due to missed minute target {0}", String.valueOf(targetExecuteMinute));
        }

        // Set for re-evaluation in the next calendar minute
        targetExecuteMinute = getMinuteMillisFromNow(1);

        taskManager.run();
      }


    } catch (Exception e) {
      error("Caught exception in primary thread:\n{0}\n", Throwables.getStackTraceAsString(e));
      System.exit(1);
    }

    System.exit(0);
  }

  private static long getMinuteMillisFromNow(final int minuteIncrement) {
    return DateTime.now().plusMinutes(minuteIncrement).withSecondOfMinute(0).withMillisOfSecond(0).getMillis();
  }

  private static void printHelp() {
    System.out.println("OMICRON - A drop-in replacement for vanilla cron on most unix systems");
    System.out.println("usage: java -jar omicron.jar <omicron config path: defaults to /etc/omicron/omicron.conf>");
    System.out.println("Pass '?' as a parameter prints this message");
  }

  private static boolean configurationUpdated(final Crontab crontab, final Configuration configuration) {
    checkNotNull(configuration, "configuration");
    checkNotNull(crontab, "crontab");

    return
      Utils.getTimestampFromPath(configuration.getConfigFilePath()) > configuration.getConfigurationTimestamp()
        ||
        Utils.getTimestampFromPath(configuration.getString(ConfigKey.CrontabPath)) > crontab.getCrontabTimestamp();
  }
}
