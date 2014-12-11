package com.zulily.omicron;

import java.io.File;

public class Utils {
  public static boolean fileExistsAndCanRead(final File file){

    return file != null && file.exists() && file.isFile() && file.canRead();
  }
}
