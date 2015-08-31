package com.zulily.omicron.scheduling;

import com.zulily.omicron.Utils;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class RunningTaskTest {

  @Test
  public void testGetPidList() throws IOException {
    Process process = new ProcessBuilder("su", "-", "cbrophy", "-c", "echo \"hello\" & sleep 3600").start();

    long parentPid = RunningTask.determinePid(process);

    Set<Long> allPids = RunningTask.recursivelyFindAllChildren(parentPid);

    System.out.println(Utils.COMMA_JOINER.join(allPids));
  }
}
