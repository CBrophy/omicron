package com.zulily.omicron;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.File;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
  private final static Logger LOG = Logger.getGlobal();
  public final static Splitter CSV_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
  public final static Splitter EQUAL_SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();
  public final static Splitter DOT_SPLITTER = Splitter.on('.').trimResults().omitEmptyStrings();


  public static boolean fileExistsAndCanRead(final File file) {
    return file != null && file.exists() && file.isFile() && file.canRead();
  }

  private static void log(final Level level, final String message, final String... args) {
    LOG.log(level, message, args);
  }

  public static void info(final String message, final String... args) {
    log(Level.INFO, message, args);
  }

  public static void info(final String message) {
    info(message, "");
  }

  public static void warn(final String message, final String... args) {
    log(Level.WARNING, message, args);
  }

  public static void warn(final String message) {warn(message, "");}

  public static void error(final String message, final String... args) {
    log(Level.SEVERE, message, args);
  }

  public static void error(final String message) {
    error(message, "");
  }

  public static String getHostName() {
    return System.getenv().containsKey("HOSTNAME") ?
      Iterables.getFirst(DOT_SPLITTER.split(System.getenv("HOSTNAME")), getInetHost()) :
      getInetHost();
  }

  private static String getInetHost() {
    try {
      return Iterables.getFirst(DOT_SPLITTER.split(InetAddress.getLocalHost().getHostName()), "UNKNOWN_HOST");
    } catch (Exception e) {
      return "UNKNOWN_HOST";
    }
  }

}
