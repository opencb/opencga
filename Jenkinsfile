pipeline {
    agent any
    stages {

        stage ('Validate ARM Templates') {
            when {
              changeset 'opencga-app/app/scripts/azure/arm/**/*.json'
            }
            steps {
                sh 'cd opencga-app/app/scripts/azure/arm && npm install armval && node node_modules/.bin/armval "**/azuredeploy.json" && rm -rf node_modules && rm -rf package-lock.json'
            }
        }


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

        stage ('Quick Test') {
            when {
                allOf {
                    changeset '**/*.java'
                    not {
                        changeset 'opencga-storage/**/*.java'
                    }
                }
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

        stage ('Complete Test') {
            when {
              changeset 'opencga-storage/**/*.java'
            }
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