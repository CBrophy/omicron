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
package com.zulily.omicron.crontab;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class representation of a variable definition in the crontab file
 * i.e. $HELLO = "hello world"
 */
public class CronVariable {
  private final String name;
  private final String value;
  private final String patternRegex;

  /**
   * Constructor
   *
   * @param name  The name of the variable
   * @param value The value of the variable
   */
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

  /**
   * @return The variable name
   */
  public String getName() {
    return name;
  }

  /**
   * @return The variable value
   */
  public String getValue() {
    return value;
  }

  String getPatternRegex() {
    return patternRegex;
  }

  /**
   * Applies the variable substitution within a cron command string
   *
   * @param line The cron command string to substitute the variable in
   * @return The command with substitutions made
   */
  public String applySubstitution(final String line) {

    return line.replaceAll(getPatternRegex(), getValue());

  }
}
