pipeline {
    agent any
    stages {
        stage ('Validate ARM Templates') {
                   options {
                       timeout(time: 5, unit: 'MINUTES')
                   }
                   steps {
                       sh 'cd opencga-app/app/cloud/azure/arm && npx --ignore-existing armval "**/azuredeploy.json"'
                   }
               }

        stage ('Test ARM Scripts') {
                   options {
                       timeout(time: 5, unit: 'MINUTES')
                   }
                   steps {
                       sh 'cd opencga-app/app/cloud/azure/arm/scripts && docker build .'
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

        stage ('Build With MongoDB storage-engine') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -Dopencga.war.name=opencga -Dcheckstyle.skip'
            }
        }

        stage ('Build and publish OpenCGA Docker Images') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                withDockerRegistry([ credentialsId: "wasim-docker-hub", url: "" ]) {
                    sh "build/cloud/docker/docker-build.py --org opencb push"
                }
            }
        }

        stage ('Build With Hadoop storage-engine against hdp2.5') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -P storage-hadoop -Phdp2.5 -Dcheckstyle.skip -Dopencga.war.name=opencga'
            }
        }
        stage ('Build With Hadoop storage-engine against hdp3.1') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -P storage-hadoop -Phdp3.1 -Dcheckstyle.skip -Dopencga.war.name=opencga'
            }
        }
        stage ('Build and publish OpenCGA Docker Images hdp3.1') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                withDockerRegistry([ credentialsId: "wasim-docker-hub", url: "" ]) {
                    sh "build/cloud/docker/docker-build.py --org opencb --images base,init push"
                }
            }
        }
        stage ('Build With Hadoop storage-engine against hdp2.6') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                sh 'mvn clean install -DskipTests -Dstorage-mongodb -Dstorage-hadoop -Phdp2.6 -DOPENCGA.STORAGE.DEFAULT_ENGINE=hadoop -Dopencga.war.name=opencga -Dcheckstyle.skip'
            }
        }
        stage ('Build and publish OpenCGA Docker Images hdp2.6') {
            options {
                timeout(time: 30, unit: 'MINUTES')
            }
            steps {
                withDockerRegistry([ credentialsId: "wasim-docker-hub", url: "" ]) {
                    sh "build/cloud/docker/docker-build.py --org opencb --images base,init push"
                }
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
