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

import com.google.common.collect.Range;
import org.junit.Test;


import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScheduleTest {
  @Test
  public void testMinuteExpression() {
    String testLine1 = "* * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine2 = "*/2 * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine3 = "1-7/7 * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine4 = "0-31 * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine5 = "0-10,20-30 * * * *   root    cd / && run-parts --report /etc/cron.hourly";

    Schedule expression1 = new CrontabExpression(1, testLine1).createSchedule();
    Schedule expression2 = new CrontabExpression(1, testLine2).createSchedule();
    Schedule expression3 = new CrontabExpression(1, testLine3).createSchedule();
    Schedule expression4 = new CrontabExpression(1, testLine4).createSchedule();
    Schedule expression5 = new CrontabExpression(1, testLine5).createSchedule();

    assertTrue(Range.closed(0, 59).containsAll(expression1.getMinutes()) && expression1.getMinutes().size() == 60);

    for (Integer minute : expression2.getMinutes()) {
      assertTrue(minute % 2 == 0);
    }
    assertEquals(expression2.getMinutes().size(), 30);

    assertTrue(expression3.getMinutes().size() == 1 && expression3.getMinutes().contains(1));

    assertTrue(Range.closed(0, 31).containsAll(expression4.getMinutes()) && expression4.getMinutes().size() == 32);

    assertEquals(expression5.getMinutes().size(), 22);

    assertTrue(Range.closed(0, 30).containsAll(expression5.getMinutes()));

    for (int i = 11; i < 20; i++) {
      assertFalse(expression5.getMinutes().contains(i));
    }
  }


  @Test
  public void testHourExpression() {
    String testLine1 = "* * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine2 = "* */2 * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine3 = "* 1-7/7 * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine4 = "* 0-15 * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine5 = "* 1-3,5-9 * * *   root    cd / && run-parts --report /etc/cron.hourly";

    Schedule expression1 = new CrontabExpression(1, testLine1).createSchedule();
    Schedule expression2 = new CrontabExpression(1, testLine2).createSchedule();
    Schedule expression3 = new CrontabExpression(1, testLine3).createSchedule();
    Schedule expression4 = new CrontabExpression(1, testLine4).createSchedule();
    Schedule expression5 = new CrontabExpression(1, testLine5).createSchedule();

    assertTrue(Range.closed(0, 23).containsAll(expression1.getHours()) && expression1.getHours().size() == 24);

    for (Integer hour : expression2.getHours()) {
      assertTrue(hour % 2 == 0);
    }
    assertEquals(expression2.getHours().size(), 12);

    for(int minute = 0; minute < 60; minute++){
      ZonedDateTime evenDateTime = ZonedDateTime.of(2015, 1, 1, 12, minute, 0, 0, ZoneId.systemDefault());
      ZonedDateTime oddDateTime = ZonedDateTime.of(2015, 1, 1, 11, minute, 0, 0, ZoneId.systemDefault());

      assertTrue(expression2.timeInSchedule(evenDateTime));
      assertFalse(expression2.timeInSchedule(oddDateTime));
    }

    assertTrue(expression3.getHours().size() == 1 && expression3.getHours().contains(1));

    assertTrue(Range.closed(0, 15).containsAll(expression4.getHours()) && expression4.getHours().size() == 16);

    assertEquals(expression5.getHours().size(), 8);

    assertTrue(Range.closed(1, 9).containsAll(expression5.getHours()));

    assertFalse(expression5.getHours().contains(4));
  }

  @Test
  public void testDayOfMonthExpression() {
    String testLine1 = "* * * * *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine2 = "* * */2 * *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine3 = "* * 1-7/7 * *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine4 = "* * 1-15 * *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine5 = "* * 1-12,14-25 * *  root    cd / && run-parts --report /etc/cron.hourly";

    Schedule expression1 = new CrontabExpression(1, testLine1).createSchedule();
    Schedule expression2 = new CrontabExpression(1, testLine2).createSchedule();
    Schedule expression3 = new CrontabExpression(1, testLine3).createSchedule();
    Schedule expression4 = new CrontabExpression(1, testLine4).createSchedule();
    Schedule expression5 = new CrontabExpression(1, testLine5).createSchedule();

    assertTrue(Range.closed(1, 31).containsAll(expression1.getDays()) && expression1.getDays().size() == 31);

    for (int i = 1; i <= 31; i += 2) {
      assertTrue(expression2.getDays().contains(i));
    }

    assertEquals(expression2.getDays().size(), 16);

    assertTrue(expression3.getDays().size() == 1 && expression3.getDays().contains(1));

    assertTrue(Range.closed(1, 15).containsAll(expression4.getDays()) && expression4.getDays().size() == 15);

    assertEquals(expression5.getDays().size(), 24);

    assertTrue(Range.closed(1, 25).containsAll(expression5.getDays()));

    assertFalse(expression5.getDays().contains(13));
  }

  @Test
  public void testMonthExpression() {
    String testLine1 = "* * * * *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine2 = "* * * */2 *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine3 = "* * * 1-7/7 *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine4 = "* * * 1-9 *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine5 = "* * * jan-may,jul-nov *  root    cd / && run-parts --report /etc/cron.hourly";

    Schedule schedule1 = new CrontabExpression(1, testLine1).createSchedule();
    Schedule schedule2 = new CrontabExpression(1, testLine2).createSchedule();
    Schedule schedule3 = new CrontabExpression(1, testLine3).createSchedule();
    Schedule schedule4 = new CrontabExpression(1, testLine4).createSchedule();
    Schedule schedule5 = new CrontabExpression(1, testLine5).createSchedule();

    assertTrue(Range.closed(1, 12).containsAll(schedule1.getMonths()) && schedule1.getMonths().size() == 12);

    assertFalse(
      schedule2.getMonths().contains(2)
        || schedule2.getMonths().contains(4)
        || schedule2.getMonths().contains(6)
        || schedule2.getMonths().contains(8)
        || schedule2.getMonths().contains(10)
        || schedule2.getMonths().contains(12)
    );

    assertEquals(schedule2.getMonths().size(), 6);

    assertTrue(schedule3.getMonths().size() == 1 && schedule3.getMonths().contains(1));

    assertTrue(Range.closed(1, 9).containsAll(schedule4.getMonths()) && schedule4.getMonths().size() == 9);

    assertEquals(schedule5.getMonths().size(), 10);

    assertTrue(Range.closed(1, 11).containsAll(schedule5.getMonths()));

    assertFalse(schedule5.getMonths().contains(6));
  }

  @Test
  public void testDayOfWeekExpression() {
    String testLine1 = "* * * * *  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine2 = "* * * * */2  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine3 = "* * * * 1-6/6  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine4 = "* * * * 2-6  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine5 = "* * * * sun-tue,thu-sat  root    cd / && run-parts --report /etc/cron.hourly";
    String testLine6 = "* * * * fri-tue  root    cd / && run-parts --report /etc/cron.hourly";

    Schedule expression1 = new CrontabExpression(1, testLine1).createSchedule();
    Schedule expression2 = new CrontabExpression(1, testLine2).createSchedule();
    Schedule expression3 = new CrontabExpression(1, testLine3).createSchedule();
    Schedule expression4 = new CrontabExpression(1, testLine4).createSchedule();
    Schedule expression5 = new CrontabExpression(1, testLine5).createSchedule();

    CrontabExpression expression6 = new CrontabExpression(1, testLine6);

    assertTrue(expression6.isMalformed());

    assertTrue(Range.closed(0, 6).containsAll(expression1.getDaysOfWeek()) && expression1.getDaysOfWeek().size() == 7);

    assertFalse(
      expression2.getDaysOfWeek().contains(1)
        || expression2.getDaysOfWeek().contains(3)
        || expression2.getDaysOfWeek().contains(5)
    );

    assertEquals(expression2.getDaysOfWeek().size(), 4);

    assertTrue(expression3.getDaysOfWeek().size() == 1 && expression3.getDaysOfWeek().contains(1));

    assertTrue(Range.closed(2, 6).containsAll(expression4.getDaysOfWeek()) && expression4.getDaysOfWeek().size() == 5);

    assertEquals(expression5.getDaysOfWeek().size(), 6);

    assertTrue(Range.closed(0, 6).containsAll(expression5.getDaysOfWeek()));

    assertFalse(expression5.getDaysOfWeek().contains(3));
  }

}
