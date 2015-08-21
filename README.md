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

1.0
* Can be configured to prevent the same job from executing endlessly if the last scheduled execution is still running.
* Can be configured to send alerts if a job has been unsuccessful after a given amount of time
* Will send all-clear success alerts if a job alert recovers on its own
* Email-based alerting or log-based alerting
* Can specify a timezone for evaluation of job schedules
* Per-job configuration of config parameters in crontab - still compatible with crond
* Tracks statistics of jobs as they execute
* Logs contain exact command being executed for helpful task debugging (variable substitution complete)


Notes & Concerns
================

* SECURITY: Use at your own risk.

Installation
============

* get the source, create the package
  - mvn clean package
  - shaded jar is output to target/omicron-<VERSION>-SNAPSHOT.jar
* deploy shaded jar to target system
* deploy updated omicron.conf to target system
* execute shaded jar (either via service script or as a background process)
  - java -jar <jar file> <conf location>
