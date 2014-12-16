package com.zulily.omicron.crontab;

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.zulily.omicron.Configuration;
import com.zulily.omicron.Utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.zulily.omicron.Utils.error;

public final class Crontab {
  private final ImmutableSet<CrontabExpression> crontabExpressions;
  private final ImmutableMap<String, String> variables;
  private final int badRowCount;
  private final long lastModified;

  public Crontab(final Configuration configuration){

    checkNotNull(configuration, "configuration");
    checkState(Utils.fileExistsAndCanRead(configuration.getCrontab()), "Cannot read/find crontab: ", configuration.getCrontab().getAbsolutePath());

    HashMap<String, String> variableMap = Maps.newHashMap();
    HashSet<CrontabExpression> results  = Sets.newHashSet();

    int bad = 0;

    this.lastModified = configuration.getCrontab().lastModified();

    try {
      int lineNumber = 0;

      ImmutableList<String> lines = Files.asCharSource(configuration.getCrontab(), Charset.defaultCharset()).readLines();

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

          results.add(new CrontabExpression(lineNumber, trimmed));

        } catch (Exception e){
          bad++;
          error("[Line: {0}] Failed to read crontab entry: {1}\n{2}", String.valueOf(lineNumber), trimmed, Throwables.getStackTraceAsString(e));
        }

      }

    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    this.badRowCount = bad;
    this.variables = ImmutableMap.copyOf(variableMap);
    this.crontabExpressions = ImmutableSet.copyOf(results);
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

  public ImmutableSet<CrontabExpression> getCrontabExpressions() {
    return crontabExpressions;
  }

  public int getBadRowCount() {
    return badRowCount;
  }

  public long getLastModified() {
    return lastModified;
  }

  public ImmutableMap<String, String> getVariables() {
    return variables;
  }
}
