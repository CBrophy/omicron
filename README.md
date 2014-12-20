omicron
=======

Java implementation of crond++ that works with the existing crontab format

Why not just use crond?

crond is beautiful in its simplicity, but not so great at helping development teams
manage and monitor critical jobs across instances.

* crond floods the machine with scheduled jobs even if they're slower than the configured schedule frequency.
* crond "knows" when a job is supposed to have run, but has non-existent capability to manage and monitor how
  jobs are adhering to the schedule

See sample conf/omicron.conf and conf/crontab for deployment examples

Features

1.0
* Can be configured to prevent the same job from executing endlessly if the last scheduled execution is still running.
* Can be configured to send alerts if a job has been returning failure codes for a given amount of time
* Will send all-clear success alerts if a job recovers on its own
* Email-based alerting or log-based alerting
* Can specify a timezone for evaluation of job schedules
* Per-job configuration of config parameters in crontab - still compatible with crond
* Tracks statistics of jobs as they execute


