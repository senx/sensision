# procDump

## Introduction

procDump is a tool to dump content from files on which we have only root access. This tool is limited to files located in the /proc directory.

The procDump binary available in the github repository is x86 64bits compliant. For the other platforms you have to recompile it.

## Build procDump

gcc procDump.c -o procDump

chown root:sensision XXX/procDump
chmod 4750 XXX/procDump

Usage: (/proc is implicit in path)
(sensision)>./procDump {pid}/environ

