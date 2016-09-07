# Manual Installation

This page describes manual installation of ProBoS. We strongly recommend installation using a CSD for Cloudera Manager. YMMV.

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
