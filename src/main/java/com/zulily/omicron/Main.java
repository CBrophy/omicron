package com.zulily.omicron;

import com.google.common.base.Throwables;
import com.zulily.omicron.conf.Configuration;
import com.zulily.omicron.scheduling.TaskManager;

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;

import static com.zulily.omicron.Utils.error;

public class Main {

  public static void main(final String[] args) {

    if (args == null || (args.length > 0 && args[0].contains("?"))) {
      printHelp();
      System.exit(0);
    }


    // see doc for java.util.logging.SimpleFormatter
    // format output will look like:
    // [Tue Dec 16 10:29:07 PST 2014] INFO: <message>
    System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tc] %4$s: %5$s %n");

    final File configFile = new File(args.length > 0 ? args[0].trim() : "/etc/omicron/omicron.conf");

    checkArgument(Utils.fileExistsAndCanRead(configFile), "Cannot find or read config file: %s", configFile.getAbsolutePath());

    try {

      final TaskManager taskManager = new TaskManager(new Configuration(configFile));

      // This method will block until interrupted
      taskManager.run();

      System.exit(0);
    } catch (Exception e) {
      error("Caught exception in primary thread:\n{0}\n", Throwables.getStackTraceAsString(e));
      System.exit(1);
    }

  }

  private static void printHelp() {
    System.out.println("OMICRON - A drop-in replacement for vanilla cron on most unix systems");
    System.out.println("usage: java -jar omicron.jar <omicron config path: defaults to /etc/omicron/omicron.conf>");
    System.out.println("Pass '?' as a parameter prints this message");
  }
}
