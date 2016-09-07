
# About

[ProBoS](https://probos-pbs.github.io) is an implementation of the Portable Batch System (PBS), including the standard `qsub` and `qstat` commands, built upon Hadoop YARN. It aims to bring closer the traditional HPC communities onto Hadoop-infrastructure, allowing the sharing of cluster resources while maintaining familiar tools for legacy HPC users.

# Features

ProBoS supports the basic functionality of a PBS implementation, as traditionally implemented by [Torque](http://www.adaptivecomputing.com/products/open-source/torque/), OpenPBS, or [Oracle Grid Engine](http://gridscheduler.sourceforge.net). These platforms implement a POSIX standard, and ProBoS aims to replicate the core POSIX standard functionality. During implementation, our reference was Torque, so this is the platform that ProBoS has the most similarities to. The table below summarises the main features of ProBoS, in contrast to Torque.

| _Feature_ | _Torque PBS_ | _ProBoS_ |
|---------|------------|------|
| Single node jobs | Yes | Yes |
| Array jobs | Yes | Yes |
| Parallel jobs | Yes | Yes |
| Interactive jobs | Yes | Yes |
| Hadoop YARN integration | No | Yes |
| Scheduling control | Yes | (w/ YARN) |
| Kerberos integration | (w/ OS) | Yes |
| Web UIs | No | Yes |

You can follow what's new in the latest release of ProBoS from [CHANGES.md](CHANGES.md).

# Requirements

You must have a working Hadoop YARN installation, including Kerberos authentication. We used CDH 5. It is _essential_ that you verify that your Hadoop YARN installation is working, with Kerberos, BEFORE you setup ProBoS.

# Installation

Installation of the ProBoS server can be in one of two manners:

 1. If you use Clouera Manager, you can install a CSD to automate the installation of ProBoS. This is strongly recommended. See [cloudera.md](cloudera.md) for more details.

 2. You can compile and install ProBoS yourself. See [manual_install.md](manual_install.md) for more details.


## Client installation
Firstly, you need to ensure that each user account on the cluster can ssh from nodes back to the client machine you are on without a password. There are many Internet guides on `ssh-keygen`. Moreover, each client machine and client user needs a working Kerberos installation. As above, if your Hadoop installation doesn't have working Kerberos, then you will not success.  

If you are not using the Cloudera installation method, you may have to add ProBos onto your path:

	#Add /path/to/probos/bin to the user's PATH
	#for bash:
	export PATH=$PATH:/path/to/probos/bin

# Using ProBoS to Run Jobs

You cannot submit jobs as the `yarn` user. Switch to a client user. You must perform Kerberos initialisation for your client user. You will be prompted for your password.

	kinit
	#or perhaps kinit $USER@ACTIVEDIRECTORY.DOMAIN

To view ProBoS jobs currently queued from a client:

	qstat

To submit a job:

	echo hostname > testJob.sh
	qsub -N testing testJob.sh
	qstat
	#wait for a period
	ls testJob.sh.o* testJob.sh.e*

To delete job with id 1:

	qdel 1

# Other Features

ProBoS has web UIs for both the controller (server), as well as each job. Goto `http://server:8029` to see the controller's UI, or follow the link from the ProBoS section of the Cloudera Manager web UI.

# License

ProBoS is licensed under the Apache license, 2.0. See LICENSE.txt for more details.

In addition, we ask that if you use ProBoS, you can let us know by adding your organisation's name to the list of (ProBoS users)[https://github.com/probos-pbs/probos/wiki/ProBoS-Users].

# Small Print

ProBoS was developed at the University of Glasgow - see [CREDITS.md](CREDITS.md) for more information.

ProBoS is similar in function to Portable Batch System (PBS), as implemented by TORQUE from Adaptive Computing, or OpenPBS and PBSPro. ProBoS is neither endorsed by nor affiliated with Altair Grid Solutions or Adaptive Computing. PBSPro, the commercial release of PBS, can be purchased through Altair.
