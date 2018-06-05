#Installation using Cloudera Manager for CDH

Installing ProBoS using the Cloudera Manager is the simplest way of installing. For this there are two necessities: 
 * Building and installing a Custom Service Descriptor (CSD) into Cloudera Manager
 * Pointing Cloudera Manager to install ProBoS from a repository
 
The Cloudera Manager documentation describes the [Installation of a CSD](http://www.cloudera.com/documentation/enterprise/5-4-x/topics/cm_mc_addon_services.html). Once you have a CSD setup, it is easy to setup Cloudera Manager to add a [custom repository location](http://www.cloudera.com/documentation/enterprise/5-2-x/topics/cm_ig_create_local_parcel_repo.html?scroll=cmig_topic_21_5), which will enable the ProBoS parcel to be installed. The custom repository to be added is `https://probos-pbs.github.io/releases/`


## Configuring the CSD setup to your environment


As mentioned in README.md, it is essential that you have a working Yarn with Kerberos setup before continuing. Unfortunately, we haven't quite got the syntax figured out for the Kerberos domain in the CSD. Hence, firstly, you need to edit `src/csd/descriptor/service.sdl` so that it knows that we have the Kerberos configured properly. Around line 114, adjust DCS.GLA.AC.UK to your Kerberos domain.
	
	# Clone the probos-csd directory.
	git clone https://github.com/probos-pbs/probos-csd.git
	# Adjust the Kerberos domain in src/csd/descriptor/service.sdl
	nano src/csd/descriptor/service.sdl
	# Use Maven to build the CSD jar file. The CSD file will be in the target directory.
	mvn package
	

## Installing the CSD jar file
Follow Cloudera's instructions on [installation of the CSD](http://www.cloudera.com/documentation/enterprise/5-4-x/topics/cm_mc_addon_services.html).

Roughly:

	sudo cp probos-csd/target/PROBOS-0.2.4.jar /opt/cloudera/csd/PROBOS-0.2.4.jar
	sudo service cloudera-scm-server restart

## Configuring CDH 5

CDH ships with Hadoop 2.6. This has backported fixes, but not functionality required for the safe operation of ProBoS. In particular, job outputs can go missing as rolling log aggregation is turned on by default. 

To address this, for all CDH 5 versions, you should alter/ensure your YARN configuration to have the following parameter: 

	yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds=-1