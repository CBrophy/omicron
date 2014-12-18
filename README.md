omicron
=======

Java implementation of crond++ that works with existing crontab formats

* Can be configured to prevent the same job from executing endlessly if the last scheduled execution is still running.
* Can be configured to send alerts if a job has been returning failure codes for a given amount of time
* Will send all-clear success alerts if a job recovers on its own
* Email-based alerting or log-based alerting
* Can specify a timezone for evaluation of job schedules
* Per-job configuration of config parameters in crontab - still compatible with crond
* Tracks statistics of jobs as they execute

See sample conf/omicron.conf and conf/crontab for deployment examples

TODO:
* Kill long-running tasks based on configurable max lifetime
* Allow policy selection both globally and per job
* Add policy for posting alerts to a URL endpoint
* Add alerts for commented job lines or uncommented *bad* lines


