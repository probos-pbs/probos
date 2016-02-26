#!/bin/bash

dir=`dirname $0`

echo "{";
for file in `ls $dir/../bin`;
do
	i=`basename $file`
	if [ "$i" != "probos" ]; then	
	cat << EOF
    "$i": {
      "destination": "/usr/bin/$i",
      "source": "bin/$i",
      "priority": 10,
      "isDirectory": false
    },
    
EOF
	fi
done | sed -n -e x -e '$ {s/,$//;p;x;}' -e '2,$ p' 
#source for sed one-liner to remove trailing ,: http://unix.stackexchange.com/questions/162377/sed-remove-the-very-last-occurrence-of-a-string-a-comma-in-a-file
echo "}";
