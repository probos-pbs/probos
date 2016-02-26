
# About

ProBoS is an implementation of the Portable Batch System (PBS), including the standard `qsub` and `qstat` commands, built upon Hadoop YARN. It aims to bring closer the traditional HPC communities onto Hadoop-infrastructure, allowing the sharing of cluster resources while maintaining familiar tools for legacy HPC users.

# Features

ProBoS supports the basic functionality of a PBS implementation, as traditionally implemented by <a href="http://www.adaptivecomputing.com/products/open-source/torque/">Torque</a>, OpenPBS, or <a href="http://gridscheduler.sourceforge.net">Oracle Grid Engine</a>. These platforms implement a POSIX standard, and ProBoS aims to replicate the core POSIX standard functionality. During implementation, our reference was Torque, so this is the platform that ProBoS has the most similarities to. The table below summarises the main features of ProBoS, in contrast to Torque.

| _Feature_ | _Torque PBS_ | _ProBoS_ |
|---------|------------|------|
| Single node jobs | Yes | Yes |
| Array jobs | Yes | Yes |
| Parallel jobs | Yes | Yes |
| Interactive jobs | Yes | Yes |
| Hadoop YARN integration | No | Yes |
| Scheduling control | Yes | (w/ YARN) |
| Kerberos integration | (w/ OS) | Yes |

# Requirements

You must have a working Hadoop YARN installation, including Kerberos authentication. We used CDH 5. It is _essential_ that you verify that your Hadoop YARN installation is working with Kerberos BEFORE you setup ProBoS.

# Installation

## Compiling

	git clone URL
	cd probos 
	mvn package

## Server installation

	#alter conf/probos-site.xml as appropriate
	#alter conf/probos-env.sh to point to a JVM with the appropriate Keberos support
	#su to the yarn user
	#pbs_server must also have access to the yarn Kerberos keytab file
	bin/pbs_server


## Client installation
Firstly, you need to ensure that each user account on the cluster can ssh from nodes back to the client machine you are on without a password. There are many Internet guides on `ssh-keygen`. Moreover, each client machine and client user needs a working Kerberos installation. As above, if your Hadoop installation doesn't have working Kerberos, then you will not success.  

Once this works, 

	#Add /path/to/hbps/bin to the user's PATH
	#for bash:
	export PATH=$PATH:/path/to/probos/bin

# Testing your installation

You cannot submit jobs as the yarn user. Switch to a client user. You must perform Kerberos initialisation for your client user. You will be prompted for your password.

	kinit -r 7d
	#or perhaps kinit -r 7d $USER@ACTIVEDIRECTORY.DOMAIN

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