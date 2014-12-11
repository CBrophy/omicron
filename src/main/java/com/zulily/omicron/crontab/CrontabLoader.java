package com.zulily.omicron.crontab;

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.zulily.omicron.Configuration;
import com.zulily.omicron.ScheduledTask;
import com.zulily.omicron.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class CrontabLoader {

  public static ImmutableMap<String, ScheduledTask> load(final Configuration configuration){
    checkNotNull(configuration, "configuration");
    checkState(Utils.fileExistsAndCanRead(configuration.getCrontab()), "Cannot read/find crontab: ", configuration.getCrontab().getAbsolutePath());

    HashMap<String, ScheduledTask> results = Maps.newHashMap();

    try {
      int lineNumber = 0;
      ImmutableList<String> lines = Files.asCharSource(configuration.getCrontab(), Charset.defaultCharset()).readLines();

      HashMap<String, String> variableMap = Maps.newHashMap();

      for (String line : lines) {
        lineNumber++;
        String trimmed = line.trim();

        // Skip commented lines
        if(trimmed.isEmpty() || '#' == trimmed.charAt(0)){
          continue;
        }

        // If it's a variable assignment, save it in the map
        List<String> variableParts = getVariable(trimmed);

        if(variableParts.size() == 2){
          variableMap.put(variableParts.get(0), variableParts.get(1));
          continue;
        }

        try {

          CrontabExpression crontabExpression = new CrontabExpression(trimmed);

          String substitutedCommand = substituteVariables(crontabExpression.getCommand(), variableMap);

          ScheduledTask scheduledTask = new ScheduledTask(crontabExpression, substitutedCommand, lineNumber, configuration);

          results.put(String.valueOf(lineNumber), scheduledTask);

        } catch (Exception e){
          System.out.println(String.format("[Line: %s] Failed to read crontab entry: %s", lineNumber, trimmed));
        }

      }

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return ImmutableMap.copyOf(results);
  }

  private static String substituteVariables(final String line, final HashMap<String, String> variableMap){
    String substituted = line;
    for (Map.Entry<String, String> variableEntry : variableMap.entrySet()) {
       substituted = substituted.replace(variableEntry.getKey(), variableEntry.getValue());
    }
    return substituted;
  }

  private static  List<String> getVariable(final String line){
    int firstEqualIndex = line.indexOf('=');

    if(firstEqualIndex == -1){
      return ImmutableList.of();
    }

    int firstQuoteIndex = line.indexOf('"');

    String varName = line.substring(0,firstEqualIndex);

    // Variable names cannot contain whitespace
    if(CharMatcher.WHITESPACE.matchesAnyOf(varName)){
      return ImmutableList.of();
    }

    String varValue = null;

    if(firstQuoteIndex > -1){

      // If first quote comes before first equal sign, cannot be a var assignment
      if(firstQuoteIndex < firstEqualIndex){
        return ImmutableList.of();
      }

      varValue = line.substring(firstQuoteIndex + 1, line.lastIndexOf('"'));
    } else {
      varValue = line.substring(firstEqualIndex + 1, line.length());
    }

    return ImmutableList.of("$" + varName, varValue);
  }
}
