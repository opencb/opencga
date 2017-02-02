OpenCGA uses FitNesse along with RestFixture to write and execute Acceptance Tests. Each Test page is an independent collection of tests and can be either executed independently or as a part of Suite run. 
Download Acceptance Tests Code:

Once user has cloned OpenCGA from git repository using following command :

$ git clone https://github.com/opencb/opencga.git

There will be a directory "opencga/opencga-test/fitnesse". This contains  a pom file and fitnesse folder with test code. 

Install Dependencies:

The following command will download all the required files to run fitnesse tests locally and put them into a folder named "dependencies" 

$ mvn clean install

Start FitNesse Server

The following command will start FitNesse server on port 7070:

/opencga-test/$ ./target/appassembler/bin/opencga-fitnesse.sh

After successful start of server, User can start web browser : http://localhost:7070/ and will see the following webpage :


