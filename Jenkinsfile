pipeline {
  agent any
  stages {
    stage('RUN JFROG CLI') {
      steps {
        sh '''export M2_HOME=/usr/share/java/maven-3
cd spark232job-mod
jfrog rt   mvn clean install
cd ..

cd spark232worker-mod
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