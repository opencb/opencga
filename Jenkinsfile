pipeline {
    agent any
    stages {
        stage ('Validate ARM Templates') {
                   options {
                       timeout(time: 5, unit: 'MINUTES')
                   }
                   steps {
                       sh 'cd opencga-app/app/scripts/azure/arm && npx --ignore-existing armval "**/azuredeploy.json"'
                   }
               }

        stage ('Test ARM Scripts') {
                   options {
                       timeout(time: 5, unit: 'MINUTES')
                   }
                   steps {
                       sh 'cd opencga-app/app/scripts/azure/arm/scripts && docker build .'
                   }
               }


        stage ('Build opencga-storage-hadoop-deps') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -f opencga-storage/opencga-storage-hadoop/opencga-storage-hadoop-deps -Dcheckstyle.skip'
            }
        }

        stage ('Build With Hadoop Profile against hdp2.5') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -Phdp2.5 -Dcheckstyle.skip'
            }
        }

        stage ('Build With Hadoop Profile against hdp3.1') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -Phdp3.1 -Dcheckstyle.skip'
            }
        }
        stage ('Build With Hadoop Profile against hdp2.6') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -Dstorage-mongodb -Dstorage-hadoop -Phdp2.6 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop -Dopencga.war.name=opencga -Dcheckstyle.skip'
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

        stage ('Build OpenCGA-Hadoop Docker Images') {
            options {
                timeout(time: 25, unit: 'MINUTES')
            }
            steps {
                sh 'make -f opencga-app/app/scripts/docker/Makefile DOCKER_ORG="opencb"'
            }
        }

        stage ('Publish OpenCGA-Hadoop Docker Images') {
             options {
                    timeout(time: 25, unit: 'MINUTES')
             }
             steps {
                script {
                   def images = ["opencga", "opencga-app", "opencga-daemon", "opencga-init", "opencga-batch"]
                   def tag = sh(returnStdout: true, script: "git log -1 --pretty=%h").trim()
                   withDockerRegistry([ credentialsId: "wasim-docker-hub", url: "" ]) {
                       for(int i =0; i < images.size(); i++){
                           sh "docker tag '${images[i]}' opencb/'${images[i]}':${tag}"
                           sh "docker push opencb/'${images[i]}':${tag}"
                       }
                   }
                }
             }
        }

  stage ('Build With Mongo Profile') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -Dopencga.war.name=opencga -Dcheckstyle.skip'
            }
        }

        stage ('Build OpenCGA-Mongo Docker Images') {
            options {
                timeout(time: 25, unit: 'MINUTES')
            }
            steps {
                sh 'docker build -t opencga-next -f opencga-app/app/scripts/docker/opencga-next/Dockerfile .'
                sh 'docker build -t opencga-demo -f opencga-app/app/scripts/docker/opencga-demo/Dockerfile .'
            }
        }

  stage ('Publish OpenCGA-Mongo Docker Images') {
             options {
                    timeout(time: 25, unit: 'MINUTES')
             }
             steps {
                script {
                   def images = ["opencga-next", "opencga-demo"]
                   def tag = sh(returnStdout: true, script: "git tag --sort version:refname | tail -1").trim().substring(1)
                   withDockerRegistry([ credentialsId: "wasim-docker-hub", url: "" ]) {
                       for(int i =0; i < images.size(); i++){
                           sh "docker tag '${images[i]}' opencb/'${images[i]}':${tag}"
                           sh "docker push opencb/'${images[i]}':${tag}"
                       }
                   }
                }
             }
        }

        stage ('Clean Docker Images') {
            options {
                timeout(time: 10, unit: 'MINUTES')
            }
            steps {
                sh 'docker system prune --force -a'
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
