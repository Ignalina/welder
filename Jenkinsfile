pipeline {
  agent any
  stages {
    stage('RUN JFROG CLI') {
      steps {
        sh '''export M2_HOME=/usr/share/java/maven-3
export JFROG_CLI_BUILD_NUMBER=${BUILD_NUMBER}
export JFROG_CLI_BUILD_NAME=${JOB_NAME}

rm -rf ~/.m2/repository/dk/ignalina/lab/


cd spark232job-mod
jfrog rt   mvn clean install -U
cd ..

cd spark232worker-mod
jfrog rt   mvn clean install

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