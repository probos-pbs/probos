#Changes

v0.2.9 - February 2015
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