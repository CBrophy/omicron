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

package com.zulily.omicron.alert;

import com.zulily.omicron.LogEntry;

public class AlertLogEntry extends LogEntry {

  private final AlertStatus status;
  private final long jobId;

  public AlertLogEntry(final long jobId, final AlertStatus status) {
    super();
    this.status = status;
    this.jobId = jobId;
  }

  public AlertStatus getStatus() {
    return status;
  }

  public long getJobId() {
    return jobId;
  }

}
