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

package com.zulily.omicron.scheduling;

import com.zulily.omicron.LogEntry;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskLogEntry extends LogEntry {

  private final int taskId;
  private final TaskStatus taskStatus;

  TaskLogEntry(final int taskId, final TaskStatus taskStatus, final long timestamp) {
    super(timestamp);
    this.taskId = taskId;
    this.taskStatus = checkNotNull(taskStatus, "taskStatus");
  }

  public int getTaskId() {
    return taskId;
  }

  public TaskStatus getTaskStatus() {
    return taskStatus;
  }

  @Override
  public String toString() {
    return String.valueOf(getTimestamp()) + ":" + String.valueOf(taskId) + ":" + taskStatus;
  }
}
