#!/bin/bash

stamp=$1
if [ "$stamp" == "" ];
then
	echo "please provide the stamp" >&2
	exit
fi

dirname=`dirname $0`
project_loc=$dirname/..
version=`cd $project_loc && mvn -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec -q`

cat > $project_loc/meta/parcel.json  <<EOF
{
  "schema_version": 1,
  "name": "ProBoS",
  "version": "$version.$stamp",
  "setActiveSymlink": true,
  "depends": "CDH",
  "conflicts": "",

  "provides" : [ "probos" ],

   "packages": [
    { "name":    "ProBoS",
      "version": "0.3.1-SNAPSHOT"
    }
   ],

   "scripts": {
    "defines": "probos_env.sh"
  },

   "components": [
    { "name":     "ProBoS",
      "version":  "$version",
      "pkg_version":  "$version",
      "pkg_release": "$version"
    }
   ],

  "users": {
    "probos": {
      "longname"    : "ProBoS daemon",
      "home"        : "/var/lib/probos",
      "shell"       : "/bin/bash",
      "extra_groups": [ "hadoop" ]
    }
  },

  "groups": [
    "hadoop"
  ]
 }
EOF
echo $version.$stamp
