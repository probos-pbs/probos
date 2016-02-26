
##commands


| Commands | Implemented? | Planned? |
|----|--------|--------|
| `qsub` | Yes | No |
| `qstat` | Yes | None |
| `qdel` | Yes | | delete array task |
| `qselect` | Yes | (improvements) |
| `pbsnodes` | Yes | (improvements) |
| `qmgr` | No | No |
| `qrerun` | No | No |
| `qalter` | No | Parts thereof |


##architecture
- client commands, run through a common probos shell script.
- Each command talks to a _controller_, the direct equivalent of Torque's pbs_server, which assigns job ids and records the state of each job.
- The controller prepares a Kitten definition of the job, which is then submitted to the Hadoop YARN ResourceManager.
- The controller talks to the Hadoop YARN ResourceManager for job management functions.
- Kitten defines an ApplicationMaster for the PBS job, which is slightly customised with reporting options for the purpose of the controller.