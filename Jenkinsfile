pipeline {
  agent any
  stages {
    stage('RUN JFROG CLI') {
      steps {
        sh '''export M2_HOME=/usr/share/java/maven-3

rm -rf ~/.m2/repository/dk/ignalina/lab/


cd spark232job-mod
mvn clean install -U
ls -l target
cd ..


ssh hadoop@10.1.1.190 \'/home/hadoop/welder_load.sh\'

'''
      }
    }

  }
  environment {
    JFROG_CLI_BUILD_NAME = 'JOB_NAME'
    JFROG_CLI_BUILD_NUMBER = 'BUILD_NUMBER'
  }
}