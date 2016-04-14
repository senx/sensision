#!/bin/sh
#
# Build the distribution .tgz for Sensision Service
#

VERSION=$1

SENSISION_HOME=sensision-${VERSION}

ARCHIVE=./archive

# Remove existing archive dir
rm -rf ${ARCHIVE}

# Create the directory hierarchy 
mkdir ${ARCHIVE}
cd ${ARCHIVE}
mkdir -p ${SENSISION_HOME}/data/targets
mkdir -p ${SENSISION_HOME}/data/metrics
mkdir -p ${SENSISION_HOME}/data/queued
mkdir -p ${SENSISION_HOME}/etc
mkdir -p ${SENSISION_HOME}/bin
mkdir -p ${SENSISION_HOME}/scripts/60000

# Copy scripts
# zk, haproxy,... are disabled by default: do not work with a standalone instance of Warp 10
cp ../src/main/groovy/*.groovy ${SENSISION_HOME}/scripts/60000
cp ../src/main/shell/zk-sensision.sh ${SENSISION_HOME}/scripts

# Copy init and startup scripts
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/shell/sensision.init > ${SENSISION_HOME}/bin/sensision.init
sed -e "s/@VERSION@/${VERSION}/g" ../src/main/shell/sensision.bootstrap > ${SENSISION_HOME}/bin/sensision.bootstrap

# Copy template configuration
sed -e "s/@VERSION@/${VERSION}/g" ../../etc/sensision.template > ${SENSISION_HOME}/etc/sensision.template

# Copy jar
cp ../build/libs/sensision-service-full-${VERSION}.jar ${SENSISION_HOME}/bin/sensision-${VERSION}.jar

# Fix permissions
chmod 755 ${SENSISION_HOME}/bin
chmod 755 ${SENSISION_HOME}/etc
chmod 755 ${SENSISION_HOME}/data
chmod 1733 ${SENSISION_HOME}/data/targets
chmod 1733 ${SENSISION_HOME}/data/metrics
chmod 700 ${SENSISION_HOME}/data/queued
chmod -R 755 ${SENSISION_HOME}/scripts
chmod 644 ${SENSISION_HOME}/bin/sensision-${VERSION}.jar
chmod 755 ${SENSISION_HOME}/bin/sensision.init
chmod 755 ${SENSISION_HOME}/bin/sensision.bootstrap

# Build tar
tar zcpf ../build/libs/sensision-service-${VERSION}.tar.gz ${SENSISION_HOME}
