#!/bin/bash
mkdir -p /tmp/bdr/output
mkdir -p /tmp/bdr/input
cd /tmp/bdr/input
for i in `seq 1 36`; do 
	echo $i;
	dd if=/dev/urandom of=$i count=16384 bs=16384; 
done

