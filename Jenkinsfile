#!/usr/bin/env groovy
import hudson.model.*

pipeline {
    agent any
    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '3'))
    }
    environment {
        version = "${getVersion()}"
        BINTRAY_USER = getParam('BINTRAY_USER')
        BINTRAY_API_KEY = getParam('BINTRAY_API_KEY')
    }
    stages {

        stage('Checkout') {
            steps {
                this.notifyBuild('STARTED', version)
                git credentialsId: 'github', poll: false, url: 'git@github.com:cityzendata/sensision.git'
                echo "Building ${version}"
            }
        }

        stage('Build') {
            steps {
                sh './gradlew clean build -x test'
            }
        }

        stage('Test') {
            steps {
                sh './gradlew test'
                junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'
                step([$class: 'JUnitResultArchiver', allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'])
            }
        }

        stage('Package') {
            steps {
                sh './gradlew sensisionLib sensisionService -x test'
                archiveArtifacts "sensision/build/libs/*.jar"
            }
        }

        stage('Deploy') {
            when {
                expression { return isItATagCommit() }
            }
            parallel {
                stage('Deploy to Bintray') {
                    options {
                        timeout(time: 2, unit: 'HOURS')
                    }
                    input {
                        message 'Should we deploy to Bintray?'
                    }
                    steps {
                        sh './gradlew bintray'
                        this.notifyBuild('PUBLISHED', version)
                    }
                }
            }
        }
    }
    post {
        success {
            this.notifyBuild('SUCCESSFUL', version)
        }
        failure {
            this.notifyBuild('FAILURE', version)
        }
        aborted {
            this.notifyBuild('ABORTED', version)
        }
        unstable {
            this.notifyBuild('UNSTABLE', version)
        }
    }
}

void notifyBuild(String buildStatus, String version) {
    // build status of null means successful
    buildStatus = buildStatus ?: 'SUCCESSFUL'
    String subject = "${buildStatus}: Job ${env.JOB_NAME} [${env.BUILD_DISPLAY_NAME}] | ${version}" as String
    String summary = "${subject} (${env.BUILD_URL})" as String
    // Override default values based on build status
    if (buildStatus == 'STARTED') {
        color = 'YELLOW'
        colorCode = '#FFFF00'
    } else if (buildStatus == 'SUCCESSFUL') {
        color = 'GREEN'
        colorCode = '#00FF00'
    } else if (buildStatus == 'PUBLISHED') {
        color = 'BLUE'
        colorCode = '#0000FF'
    } else {
        color = 'RED'
        colorCode = '#FF0000'
    }

    // Send notifications
    this.notifySlack(colorCode, summary, buildStatus)
}

void notifySlack(String color, String message, String buildStatus) {
    String slackURL = getParam('slackUrl')
    String payload = "{\"username\": \"${env.JOB_NAME}\",\"attachments\":[{\"title\": \"${env.JOB_NAME} ${buildStatus}\",\"color\": \"${color}\",\"text\": \"${message}\"}]}" as String
    sh "curl -X POST -H 'Content-type: application/json' --data '${payload}' ${slackURL}" as String
}

String getParam(String key) {
    return params.get(key)
}

String getVersion() {
    return sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
}

boolean isItATagCommit() {
    String lastCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
    String tag = sh(returnStdout: true, script: "git show-ref --tags -d | grep ^${lastCommit} | sed -e 's,.* refs/tags/,,' -e 's/\\^{}//'").trim()
    return tag != ''
}
