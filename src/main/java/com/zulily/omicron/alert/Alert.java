package com.zulily.omicron.alert;

import com.google.common.collect.ComparisonChain;
import com.zulily.omicron.conf.Configuration;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Alert implements Comparable<Alert> {
  private final boolean failed;
  private final long timstamp;
  private final String policyName;
  private final String message;
  private final int lineNumber;
  private final String commandLine;
  private long lastAlertTimestamp = Configuration.DEFAULT_TIMESTAMP;

  public Alert(final String policyName,
               final String message,
               final int lineNumber,
               final String commandLine,
               final boolean failed) {

    this.timstamp = DateTime.now().getMillis();
    this.policyName = checkNotNull(policyName, "policyName");
    this.message = checkNotNull(message, "message");
    this.lineNumber = lineNumber;
    checkArgument(lineNumber > 0, "lineNumber must be positive");

    this.commandLine = checkNotNull(commandLine, "commandLine");
    this.failed = failed;
  }

  public long getTimstamp() {
    return timstamp;
  }

  public String getPolicyName() {
    return policyName;
  }

  public String getMessage() {
    return message;
  }

  public int getLineNumber() {
    return lineNumber;
  }

  public String getCommandLine() {
    return commandLine;
  }

  public long getLastAlertTimestamp() {
    return lastAlertTimestamp;
  }

  public void setLastAlertTimestamp(long lastAlertTimestamp) {
    this.lastAlertTimestamp = lastAlertTimestamp;
  }

  @Override
  public int compareTo(Alert o) {
    checkNotNull(o, "compare to null instance");

    return ComparisonChain.start()
      .compare(timstamp, o.timstamp)
      .compare(policyName, o.policyName)
      .compare(message, o.message)
      .result();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Alert
      && this.timstamp == ((Alert) o).timstamp
      && this.policyName.equals(((Alert) o).policyName)
      && this.message.equals(((Alert) o).message)
      && this.lineNumber == ((Alert) o).lineNumber
      && this.commandLine.equals(((Alert) o).commandLine);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(this.timstamp);
  }

  public boolean isFailed() {
    return failed;
  }
}
