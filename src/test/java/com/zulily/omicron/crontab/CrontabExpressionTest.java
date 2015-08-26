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


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CrontabExpressionTest {

  @Test
  public void testExecutingUser() {
    String testLine1 = "* * * * *  root    cd / && run-parts --report /etc/cron.hourly";
    CrontabExpression expression1 = new CrontabExpression(1, testLine1);

    assertEquals("root", expression1.getExecutingUser());
  }

  @Test
  public void testCommand() {
    String testLine1 = "* * * * *  root    cd  /  && run-parts --report /etc/cron.hourly";
    CrontabExpression expression1 = new CrontabExpression(1, testLine1);

    assertEquals("cd / && run-parts --report /etc/cron.hourly", expression1.getCommand());
  }

  @Test
  public void commentedAndUncommentedExpressionsDoNotMatch() {
    String testLine1 = "* * * * *  root    cd  /  && run-parts --report /etc/cron.hourly";
    String testLine2 = "#* * * * *  root    cd  /  && run-parts --report /etc/cron.hourly";

    CrontabExpression expression1 = new CrontabExpression(1, testLine1);
    CrontabExpression expression2 = new CrontabExpression(2, testLine2);

    assertFalse(expression1.equals(expression2));
  }

  @Test
  public void testMultipleLeadingHashMarks() {
    String testLine1 = "# # #* * * * *  root    cd  /  && run-parts --report /etc/cron.hourly";
    String testLine2 = "##* * * * *  root    cd  /  && run-parts --report /etc/cron.hourly";
    String testLine3 = "#* * * * *  root    cd  /  && run-parts --report /etc/cron.hourly";
    String testLine4 = "* * * * *  root    cd  /  && run-parts --report /etc/cron.hourly";

    CrontabExpression expression1 = new CrontabExpression(1, testLine1);
    CrontabExpression expression2 = new CrontabExpression(2, testLine2);
    CrontabExpression expression3 = new CrontabExpression(3, testLine3);
    CrontabExpression expression4 = new CrontabExpression(4, testLine4);

    assertFalse(expression1.equals(expression4));
    assertFalse(expression2.equals(expression4));
    assertFalse(expression3.equals(expression4));

    assertTrue(expression1.equals(expression2));
    assertTrue(expression1.equals(expression3));
    assertTrue(expression2.equals(expression3));

  }

}