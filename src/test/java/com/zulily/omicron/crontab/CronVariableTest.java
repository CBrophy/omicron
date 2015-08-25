package com.zulily.omicron.crontab;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CronVariableTest {

  @Test
  public void testVariableSubstitution() {
    CronVariable var1 = new CronVariable("VAR1", "test1");
    CronVariable var = new CronVariable("VAR", "test");

    String testLine = "$VAR $the $quick $VAR1 $brown $VARfox $VAR1jumped $over $the $VAR $lazy $dog $VAR1";

    assertEquals(var.applySubstitution(testLine), "test $the $quick $VAR1 $brown $VARfox $VAR1jumped $over $the test $lazy $dog $VAR1");
    assertEquals(var1.applySubstitution(testLine), "$VAR $the $quick test1 $brown $VARfox $VAR1jumped $over $the $VAR $lazy $dog test1");
    assertEquals(var1.applySubstitution(var.applySubstitution(testLine)), "test $the $quick test1 $brown $VARfox $VAR1jumped $over $the test $lazy $dog test1");
  }

}