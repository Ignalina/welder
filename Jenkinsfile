pipeline {
  agent any
  stages {
    stage('RUN JFROG CLI') {
      steps {
        sh '''export M2_HOME=/usr/share/java/maven-3


cd spark232job-mod
jfrog rt   mvn clean install -U
cd ..

cd spark232worker-mod
jfrog rt   mvn clean install

cd ..


ssh spark@10.1.1.190 \'/home/hadoop/welder_load.sh\'

'''
      }
    }

  }
  environment {
    JFROG_CLI_BUILD_NAME = '${env.JOB_NAME}'
    JFROG_CLI_BUILD_NUMBER = '${env.BUILD_NUMBER}'
  }
}