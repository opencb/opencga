pipeline {
    agent any
    stages {

        stage ('Validate') {
            steps {
                sh 'mvn validate'
            }
        }

        stage ('Build') {
            steps {
                sh 'mvn clean install -DskipTests -Popencga-storage-hadoop-deps'
            }
        }

        stage ('Test') {
            steps {
                sh 'mvn -Dmaven.test.failure.ignore=true test'
            }
            post {
                success {
                    junit '**/target/surefire-reports/**/*.xml'
                }
            }
        }
    }
}