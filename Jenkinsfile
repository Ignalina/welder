pipeline {
  agent any
  stages {
    stage('RUN JFROG CLI') {
      steps {
        sh '''export M2_HOME=/usr/share/java/maven-3
git clone https://github.com/Ignalina/welder.git
cd welder 

cd spark232job-mod
jfrog rt   mvn clean install -U
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