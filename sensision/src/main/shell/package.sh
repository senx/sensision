#!/bin/sh
#
#   Copyright 2018  SenX S.A.S.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#
# Build the distribution .tar.gz for Sensision Service
#

VERSION=$1

SENSISION_HOME=sensision-${VERSION}

ARCHIVE=./archive

# Remove existing archive dir
rm -rf ${ARCHIVE}

# Create the directory hierarchy
mkdir ${ARCHIVE}
cd ${ARCHIVE}
mkdir -p ${SENSISION_HOME}/targets
mkdir -p ${SENSISION_HOME}/metrics
mkdir -p ${SENSISION_HOME}/queued
mkdir -p ${SENSISION_HOME}/etc
mkdir -p ${SENSISION_HOME}/logs
mkdir -p ${SENSISION_HOME}/templates
mkdir -p ${SENSISION_HOME}/bin
mkdir -p ${SENSISION_HOME}/scripts/60000

# Copy scripts
# zk, haproxy,... are disabled by default: do not work with a standalone instance of Warp 10
cp ../src/main/groovy/*.groovy ${SENSISION_HOME}/scripts/60000
cp ../src/main/shell/zk-sensision.sh ${SENSISION_HOME}/scripts

# Copy init and startup scripts
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/shell/sensision.init > ${SENSISION_HOME}/bin/sensision.init

# log4j.properties
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/log4j.properties.template > ${SENSISION_HOME}/templates/log4j.properties.template

# Copy template configuration
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/sensision.template > ${SENSISION_HOME}/templates/sensision.template

# Copy WarpScript used by TokenGen
cp ../../etc/sensision-tokengen.mc2 ${SENSISION_HOME}/templates/sensision-tokengen.mc2

# Copy jar
cp ../build/libs/sensision-service-${VERSION}.jar ${SENSISION_HOME}/bin/sensision-${VERSION}.jar

# Copy procDump tool
cp ../../procDump/procDump ${SENSISION_HOME}/bin/procDump

# Fix permissions
chmod 755 ${SENSISION_HOME}/bin
chmod 755 ${SENSISION_HOME}/etc
chmod 755 ${SENSISION_HOME}/templates
chmod 755 ${SENSISION_HOME}/logs
chmod 1733 ${SENSISION_HOME}/targets
chmod 1733 ${SENSISION_HOME}/metrics
chmod 700 ${SENSISION_HOME}/queued
chmod -R 755 ${SENSISION_HOME}/scripts
chmod 644 ${SENSISION_HOME}/bin/sensision-${VERSION}.jar
chmod 755 ${SENSISION_HOME}/bin/sensision.init
chmod 4750 ${SENSISION_HOME}/bin/procDump

# Build tar
tar zcpf ../build/libs/sensision-service-${VERSION}.tar.gz ${SENSISION_HOME}


# Remove archive dir
cd -
rm -rf ${ARCHIVE}