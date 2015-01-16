package com.zulily.omicron.crontab;

import static com.google.common.base.Preconditions.checkNotNull;

public class CronVariable {
  private final String name;
  private final String value;
  private final String patternRegex;

  CronVariable(final String name, final String value) {
    this.name = checkNotNull(name, "name");
    this.value = checkNotNull(value, "value");

    // Make sure to match ONLY the whole variable name
    // For example:
    // VAR1 = "test"
    // VAR = "hi"

    // $VAR1 should not match "$VAR" simply because one name is a
    // substring of another

    this.patternRegex = "(\\$" + name + ")(?=\\s+|$)";
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public String getPatternRegex() {
    return patternRegex;
  }

  public String applySubstitution(final String line) {

    return line.replaceAll(getPatternRegex(), getValue());

  }
}
