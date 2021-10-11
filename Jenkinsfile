pipeline {
  agent any
  stages {
    stage('RUN JFROG CLI') {
      steps {
        sh '''export M2_HOME=/usr/share/java/maven-3
cd sparkjob-mod
jfrog rt   mvn clean install -U
cd ..

cd sparkworker-mod
jfrog rt   mvn clean install

cd ..

'''
      }
    }

  }
  environment {
    JFROG_CLI_BUILD_NAME = '${env.JOB_NAME}'
    JFROG_CLI_BUILD_NUMBER = '${env.BUILD_NUMBER}'
  }
}