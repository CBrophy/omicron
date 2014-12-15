package com.zulily.omicron;

import java.io.File;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Utils {
  public static Logger LOG = LogManager.getLogManager().getLogger("omicron");

  public static boolean fileExistsAndCanRead(final File file){
    return file != null && file.exists() && file.isFile() && file.canRead();
  }

  public static void info(final String message){
    LOG.info(message);
  }

  public static void warn(final String message) {
    LOG.warning(message);
  }

  public static void error(final String message){
    LOG.severe(message);
  }
}
