package com.zulily.omicron.scheduling;

import com.zulily.omicron.Utils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class RunningTaskTest {

  @Test
  public void testProcFSWorks() throws IOException {
    Set<Long> allPids = RunningTask.recursivelyFindAllChildren(getProcFsSelfPid());

    assertTrue(allPids.size() > 0);

    allPids
      .forEach(pid -> assertTrue(pid > -1));
  }

  private static long getProcFsSelfPid() {
    try {
      String procFsPid = new File("/proc/self").getCanonicalFile().getName();
      return Utils.isNullOrEmpty(procFsPid) ? -1 : Long.parseLong(procFsPid);

    } catch (IOException e) {
      return -1;
    }
  }
}
