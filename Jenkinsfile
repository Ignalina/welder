pipeline {
  agent any
  stages {
    stage('RUN JFROG CLI') {
      steps {
        sh '''rm -rf ~/.m2/repository/dk/ignalina/lab/


'''
      }
    }

    stage('Spark232 insert only') {
      parallel {
        stage('Spark232') {
          steps {
            sh '''export M2_HOME=/usr/share/java/maven-3



cd spark232job-mod
mvn clean install -U
ls -l target
cd ..


#ssh spark@10.1.1.193 \'/home/hadoop/welder_load.sh\'

'''
          }
        }

        stage('Spark3.2.0') {
          steps {
            sh '''export M2_HOME=/usr/share/java/maven-3



cd spark320job-mod
mvn clean install -U
ls -l target
cd ..


scp target/welder-delta-job-1.0-SNAPSHOT.jar spark@10.1.1.193:.
ssh spark@10.1.1.193 \'/home/spark/welder_load_spark320.sh\'

'''
          }
        }

      }
    }

  }
  environment {
    JFROG_CLI_BUILD_NAME = 'JOB_NAME'
    JFROG_CLI_BUILD_NUMBER = 'BUILD_NUMBER'
  }
}