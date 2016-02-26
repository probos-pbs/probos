#!/bin/bash
CMD=$1

case $CMD in
  (start)
    echo "Starting the ProBoS controller, with config dir [$CONF_DIR]"
    # Source the common script to use acquire_kerberos_tgt
	. $COMMON_SCRIPT
	
	# acquire_kerberos_tgt expects that the principal to be kinited is referred to by 
	# SCM_KERBEROS_PRINCIPAL environment variable
	export SCM_KERBEROS_PRINCIPAL=probos/`hostname`
	
	# acquire_kerberos_tgt expects that the argument passed to it refers to the keytab file
	acquire_kerberos_tgt probos.keytab
    export PROBOS_CONF_DIR=$CONF_DIR
    export JAVA_HOME
    exec pbs_server
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac