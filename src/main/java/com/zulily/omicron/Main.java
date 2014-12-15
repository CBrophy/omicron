package com.zulily.omicron;

import com.google.common.base.Throwables;

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;

public class Main {

  public static void main(final String[] args) {
    if (args == null || args.length == 0) {
      printHelp();
      System.exit(0);
    }

    File configFile = new File(args.length > 0 ? args[0].trim() : "/etc/omicron/omicron.conf");

    checkArgument(Utils.fileExistsAndCanRead(configFile), "Cannot find config file: %s", configFile.getAbsolutePath());

    try {

      TaskManager taskManager = new TaskManager(new Configuration(configFile));

      taskManager.run();

      System.exit(0);
    } catch (Exception e) {
      System.out.println(Throwables.getStackTraceAsString(e));
      System.exit(1);
    }

  }

  private static void printHelp() {
    System.out.println("OMICRON - A drop-in replacement for vanilla cron");
    System.out.println("usage: java -jar omicron.jar <omicron config path: defaults to /etc/omicron/omicron.conf>");
  }
}
