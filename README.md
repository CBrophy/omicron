Omicron
=======

Copyright (C) 2014 zulily, Inc.

Read the LICENSE file for terms of use

What is Omicron?
================

Java implementation of crond with monitoring/alerting features. Works with the existing crontab file format.

Third-party dependencies are: maven, guava, joda-time, junit, and javax mail.

Current Functional Requirements
===============================

* JRE 8 or above

* Linux platform tested
  - OSX untested, might work
  - Windows untested, platform specific functions will most likely fail (su/root checks)

* Success/Fail alerting requires executable to support meaningful return codes. 0 expected to indicate success

* !!execute as user requires root privs!!

* Tested as a root-level init.d service. Note: service script not included, however service script examples abound on
  the internet

See conf/crontab and conf/omicron.conf for deployment config examples

Features
========

**1.1**

*   Multiple bug fixes regarding SLA enforcement and notification

    **Time Since Success**: do not alert more than once between scheduled task runs
    
    **Time Since Success**: do not alert if the job has a recent success and is currently running
    
*   NEW config option: **task.timeout.minutes** -> Automatically kill jobs that are taking too long
*   NEW config option: **command.path** -> Configure the location of external commands
*   NEW config option: **alert.downtime** -> Set a start time and duration for silencing alerts either system-wide or at the task level
*   BREAKING CHANGE: **task.duplicate.allowed.count** renamed to **task.max.instance.count**


**1.0**

*   Can be configured to prevent the same job from executing endlessly if the last scheduled execution is still running.
*   Can be configured to send alerts if a job has been unsuccessful after a given amount of time
*   Will send all-clear success alerts if a job alert recovers on its own
*   Email-based alerting or log-based alerting
*   Can specify a timezone for evaluation of job schedules
*   Per-job configuration of config parameters in crontab - still compatible with crond
*   Tracks statistics of jobs as they execute
*   Logs contain exact command being executed for helpful task debugging (variable substitution complete)


Notes & Concerns
================

*   SECURITY: Use at your own risk. There are multiple potential vectors for abuse. This software is not intended for use on multi-tenant or public-facing systems.

Installation
============

*   get the release source, create the package with maven
    
    _mvn clean package_
        
    shaded jar is then output to target/omicron-\<VERSION\>.jar

*   deploy shaded jar to your system of choice
*   deploy updated omicron.conf to same system
*   execute shaded jar (either via service script or as a background process)

    _java -jar \<jar file\> \<conf location\>_
