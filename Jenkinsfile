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
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk/
#export JAVA_HOME=/usr/lib/jvm/default-jvm/


gpg --list-keys

cd spark232job-mod
mvn clean install -U
#ls -l target



#ssh spark@10.1.1.190 \'/home/hadoop/welder_load.sh\'
#scp target/welder-spark-job-0.0.1.jar spark@10.1.1.190:.

#mvn deploy:deploy-file -Durl=https://nexus.x14.se/repository/maven-snapshots -Dfile=target/welder-spark-job-0.0.1-SNAPSHOT.jar -DgroupId=dk.ignalina.lab.spark232 -DartifactId=welder-spark-job -Dpackaging=jar -Dversion=0.0.1-SNAPSHOT -DrepositoryId=x14-repo


mvn deploy:deploy-file -Durl=https://nexus.x14.se/repository/maven-releases -Dfile=target/welder-spark-job-0.0.1.jar -DgroupId=dk.ignalina.lab.spark232 -DartifactId=welder-spark-job -Dpackaging=jar -Dversion=0.0.1 -DrepositoryId=x14-repo


ssh spark@10.1.1.190 \'/home/spark/welder_load_spark232.sh\'

'''
          }
        }

        stage('Spark3.2.0') {
          steps {
            sh '''export M2_HOME=/usr/share/java/maven-3
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk/
#export JAVA_HOME=/usr/lib/jvm/default-jvm/



cd spark320job-mod
mvn clean install -U
ls -l target


scp target/welder-delta-job-1.0-SNAPSHOT.jar spark@10.1.1.193:.
ssh spark@10.1.1.193 \'/home/spark/welder_load_spark320.sh\'

cd ..

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