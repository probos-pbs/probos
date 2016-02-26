#qsub

See http://pubs.opengroup.org/onlinepubs/9699919799/utilities/qsub.html and http://linux.die.net/man/1/qsub-torque 

## Comparison with Torque

|Option|POSIX?| Meaning | ProBoS Support|
|------|---|---------|--------------|
| C | Yes | Define the prefix that declares a directive to the qsub utility within the script. | Yes | 
| l | Yes | Define the resource list. | Yes | 
| N | Yes | Define the name of the job. | Yes |
| o | Yes | Defines the standard output path. | Yes | 
| e | Yes | Defines the standard error path. | Yes | 
| S | Yes | Defines the shell to use. | Partial (no per-host definitions) |
| V | Yes | Specify that all of the environment variables of the process are exported to the context of the batch job. | Yes |
| v | Yes | Add to the list of variables that are exported to the batch job.| Yes|
| z | Yes | Specify that the utility does not write the batch job_identifier of the created batch job to standard output. | Yes|
| t | No  | Specifies the task ids of a job array. | Yes |
| a |Yes| Define the time at which a batch job becomes eligible for execution. | No |
| A |Yes| Define the account to which the resource consumption of the batch job should be charged. | Yes |
| c |Yes| Define whether the batch job should be checkpointed, and if so, how often. | No |
| h | Yes | Specify that a USER hold is applied to the batch job. | Yes |
| j | Yes | Define which streams of the batch job are to be merged| Yes |
| k | Yes | Define which output of the batch job to retain on the execution host. | No |
| m | Yes | Define the points in the execution of the batch job at which the batch server that manages the batch job shall send mail about a change in the state of the batch job. | Yes | 
| M | Yes | Define the list of users to which a batch server that executes the batch job shall send mail, if the server sends mail about the batch job. | No |
| p | Yes | Define the priority the batch job should have relative to other batch jobs owned by the batch server. | No | 
| q  |Yes| Define the destination of the batch job. | No (PBS_DEFAULT var is supported) | 
| r  |Yes| Define whether the batch job is rerunnable. | No |
| u  |Yes| Define the user name under which the batch job is to execute. | No |
| b |No | Defines the maximum number of seconds qsub will block attempting to contact pbs_server. | No |
| D | No | Defines the working directory path to be used for the job. For Torque, defines the root directory to be used for the job. | No |
| f | No | Specifies that the job is fault tolerant.| No |
| I | No | Requests an interactive job. | Yes |
| T | No | Allows for per job prologue and epilogue scripts.| No |
| W| No | additional attributes, such as stage-in/stage-out, inter-job dependencies, interactive. | No |
