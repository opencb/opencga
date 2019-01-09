pipeline {
    agent any
    stages {

        stage ('Build') {
            steps {
                sh 'mvn clean install -DskipTests -Popencga-storage-hadoop-deps -Dcheckstyle.skip'
            }
        }

        stage ('Validate') {
            steps {
                sh 'mvn validate'
            }
        }

        stage ('Test') {
            when {
              changeset '**/*.java'
            }
            steps {
                sh 'mvn -Dmaven.test.failure.ignore=true test -pl \'!:opencga-storage-mongodb,!:opencga-storage-hadoop,!:opencga-storage-hadoop-core\''
            }
            post {
                success {
                    junit '**/target/surefire-reports/**/*.xml'
                }
            }
        }

        stage ('TestStorage') {
            when {
              changeset 'opencga-storage/**/*.java'
            }
            steps {
                sh 'mvn -Dmaven.test.failure.ignore=true test -pl \':opencga-storage-mongodb,:opencga-storage-hadoop,:opencga-storage-hadoop-core\''
            }
            post {
                success {
                    junit '**/target/surefire-reports/**/*.xml'
                }
            }
        }
    }
}