pipeline {
    agent any
    stages {

        stage ('Validate ARM Templates') {
            options {
                timeout(time: 5, unit: 'MINUTES')
            }
            steps {
                sh 'cd opencga-app/app/scripts/azure/arm && npm install armval && node node_modules/.bin/armval "**/azuredeploy.json" && rm -rf node_modules && rm -rf package-lock.json'
            }
        }

        stage ('Build') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -Popencga-storage-hadoop-deps -Dcheckstyle.skip -Dopencga.war.name=opencga'
            }
        }

        stage ('Validate') {
            options {
                timeout(time: 5, unit: 'MINUTES')
            }
            steps {
                sh 'mvn validate'
            }
        }

        stage ('Docker Build') {
            options {
                timeout(time: 10, unit: 'MINUTES')
            }
            steps {
                sh 'make -f opencga-app/app/scripts/docker/Makefile DOCKER_ORG="opencb"'
            }
        }

        stage ('Publish Docker Images') {
             steps {
                script {
                   def images = ["opencga", "opencga-app", "opencga-daemon", "opencga-init", "iva"]
                   def tag = sh(returnStdout: true, script: "git rev-parse --verify HEAD").trim()
                   withDockerRegistry([ credentialsId: "wasim-docker-hub", url: "" ]) {
                       for(int i =0; i < images.size(); i++){
                           sh "docker tag '${images[i]}' opencb/'${images[i]}':${tag}"
                           sh "docker push opencb/'${images[i]}':${tag}"
                       }
                   }
                }
             }
        }

        stage ('Quick Test') {
            options {
                timeout(time: 1, unit: 'HOURS')
            }
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
            options {
                timeout(time: 4, unit: 'HOURS')
            }
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