package com.zulily.omicron.crontab;

import com.google.common.collect.Range;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CrontabExpressionTest {



  @Test
  public void testMinuteExpression(){
    String testLine1 = "* * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine2 = "*/2 * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine3 = "1-7/7 * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine4 = "0-31 * * * *   root    cd / && run-parts --report /etc/cron.hourly";
    String testLine5 = "0-10,20-30 * * * *   root    cd / && run-parts --report /etc/cron.hourly";

    CrontabExpression expression1 = new CrontabExpression(testLine1);
    CrontabExpression expression2 = new CrontabExpression(testLine2);
    CrontabExpression expression3 = new CrontabExpression(testLine3);
    CrontabExpression expression4 = new CrontabExpression(testLine4);
    CrontabExpression expression5 = new CrontabExpression(testLine5);

    assertTrue(Range.closed(0,59).containsAll(expression1.getMinutes()) && expression1.getMinutes().size() == 60);

    for (Integer minute : expression2.getMinutes()) {
      assertTrue(minute % 2 == 0);
    }
    assertEquals(expression2.getMinutes().size(), 30);

    assertTrue(expression3.getMinutes().size() == 1 && expression3.getMinutes().contains(1));

    assertTrue(Range.closed(0, 31).containsAll(expression4.getMinutes()) && expression4.getMinutes().size() == 32);

    assertEquals(expression5.getMinutes().size(), 22);
  }



  @Test
  public void testHourExpression(){

  }

  @Test
  public void testDayOfMonthExpression(){

  }

  @Test
  public void testMonthExpression(){

  }

  @Test
  public void tesDayOfWeekExpression(){

  }

  @Test
  public void testExecutingUser(){

  }

  @Test
  public void testCommand(){

  }


}