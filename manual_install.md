# Manual Compilation & Installation

This page describes manual installation of ProBoS. We strongly recommend installation using a CSD for Cloudera Manager. YMMV.

## Pre-requisites

Most of the pre-requisites of ProBoS are available through Maven. We would note that (Kitten)[https://github.com/cloudera/kitten/] has several bugs thats we have fixed, along with various improvements (e.g. support for Hadoop 2.6 node labels). For this reason, before compiling ProBoS, you must install the patched version of Kitten available at https://github.com/probos-pbs/kitten:

	git clone https://github.com/probos-pbs/kitten.git
	cd kitten
	mvn install

## Compiling

	git clone https://github.com/probos-pbs/probos.git
	cd probos 
	mvn package

## Server installation

	#alter conf/probos-site.xml as appropriate
	#alter conf/probos-env.sh to point to a JVM with the appropriate Keberos support
	#su to the yarn user
	#pbs_server must also have access to the yarn Kerberos keytab file
	bin/pbs_server
