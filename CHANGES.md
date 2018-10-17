# Changes

v0.2.15 - February 2018
 * #3 HOME should be set, XDG_RUNTIME_DIR should not be set.

v0.2.14 - February 2018
 * [KITTEN] Allow ApplicationSubmissionContext to be overridden
 * #1 Container log urls have extra http appended.
 * #2 YARN's NodeManager Container rolling logs causes problems (e.g. qpeek not working) [Fix for Hadoop 2.7.0+ only]

v0.2.13 - February 2017
 * [KITTEN] Better logging when resources are cut to cluster maximums
 * Prevent job failures when /bin and /usr/bin are missing from the job's PATH
 * When a job fails, make an output file is written with YARN's diagnostic message

v0.2.12 - July 2016
 * Added metrics using metrics.dropwizard.io (aka Codahale)
 * JMX metrics exposed via HTTP RESTful interface

v0.2.11 - April 2016
 * Added tracking URL to qstat output
 * Clarified logging in Controller wrt RPC port numbers
 * Added background image to servlets
 * Added log access to job master HTTP UI
 * Maven fixes to prevent slf4j binding warnings
 * Increased timeouts for token renewal (in case a job doesn't start within 24 hours) 

v0.2.10 - March 2016
 * Mavenize more, including version numbering
 * Fix sendmail
 * Fix unit tests and controller wrt absolute file paths
 * Use SLF4j and log4j, to permit CSD to configure logging
 * Fix token renewal

v0.2.9 - February 2016
 * Tokens need to be renewed through master heartbeat
 * qsub should ignore the TERMCAP variable
 * #7: dont fill up user's home directory with probos job files
 * Added interactive job support
 * #1: Added preliminary distributed job support
 * Added Job services based on Apache Mina sshd to support interactive & distributed jobs
 * Refactored KittenUtils
 * ControllerServer now extends Guava AbstractService
 * Provided webservers for ApplicationMaster and ControllerServer
 * qsub robustness to expired jobs  

v0.2.8 - 4th November 2015
 * Fixed some qstat errors
 * Finally debugged the failure exit codes of some jobs
 * Pass the script source into the Controller, to be saved to HDFS and passed to the job space by Kitten
 * Try 2 default locations for sendmail, changed default location
 * Create HDFS directory as part of the CSD

v0.2.7 - 22nd October 2015
 * Drop env vars thats have backticks.
 * Remove some debugging code from KittenUtils
 

v0.2.6 - 14th September 2015

 * Enable qsub -H for user holds
 * Enabled logging for MailClient implementations 

v0.2.5 - 14th September 2015

 * Prevent NPE in Controller for qstat-ing of unknown jobs
 * Allow qdel -p purging
 * Logging of messages from container aborts
 * submitAll.pl should be executable

v0.2.4 - 6th September 2015

 * PBSNodeStatus should detail the jobs running on each node to support pestat
 * Added bin/ symlink for qpeek

v0.2.3 - 4th September 2015

 * Add "--help" usage options to all cli commands
 * Refine qpeek on controller side
 * Only use a core Resource for the application master on large array jobs
 * Use hadoop command to start pbs_appmaster, not yarn
 * Report ContainerIds and tracking URL in qstat -f
 * Split Utils into Utils & JobUtils
 * Job's initial working directory now defaults to `PBS_INITDIR` (c.f. `qsub -d`) or `PBS_O_HOME`
 * Add timeout to sendmail MailClient
 * Add node labels to PBSNodeStatusd & pbsnodes output
 * Add javax mail support for SMTP servers

v0.2.1, 0.2.2 

 * version bumps for CDH parcel deployments

v0.2.0 - 3rd September 2015

 * Added ProbosApplicationMaster, to report ContainerIds to the Controller
 * Added qpeek log access command
 * Added email support
 * Fixed job array support
 * Added qselect support
 * Fixed job array in Kitten
