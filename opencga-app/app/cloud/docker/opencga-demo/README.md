# OpenCGA DEMO

A standalone image with a complete opencga installation for demo and testing purposes.
Not for production usage.

Includes an embedded MongoDB and Solr.

You can optionally load some demo data. Launch the container with the env var `load=true` to load data from the [Corpasome study](https://figshare.com/articles/Corpasome/693052). This process takes from 30 to 45 minutes.

```
docker run -d --name opencga-demo --env load=true -p 9090:9090 opencb/opencga-demo:REPLACEME_OPENCGA_VERSION
```


## Interact with the container
Creating a convenient alias
```
alias opencga.sh='docker exec -it opencga-demo ./opencga.sh'
```

Then you can login (user: `demo`, password: `Demo_P4ss`) and check the progress of the data load.
```
opencga.sh users login -u demo -p
# Password: Demo_P4ss

opencga.sh jobs top
```

After 30 minutes the top should look like this:
![](img/jobs_top.png)

## Connect with IVA
You could also start an IVA to connect with this opencga-demo.

```
docker run -d --name iva-app -p 80:80 opencb/iva-app:REPLACEME_IVA_VERSION
docker exec -it iva-app bash -c 'echo "opencga.host=\"http://localhost:9090/opencga\"" >> /usr/local/apache2/htdocs/iva/conf/config.js'
```

Then, open IVA in the explorer:
[http://localhost/iva](http://localhost/iva)


## Check container activity
```
docker logs -f  opencga-demo
```

## Clean resources
```
docker rm --force --volumes opencga-demo
docker rm --force iva-app
```


# Connect with an external Hadoop cluster

Optionally, this docker could also connect to an external hadoop cluster. Just need to select the appropriate hadoop flavoured docker matching your installation and provide the connection credentials.

The credentials are shared using environment variables:

- **HADOOP_SSH_HOST** : Hadoop cluster main node.   
- **HADOOP_SSH_USER** : Ssh user.
- **HADOOP_SSH_KEY** : Ssh key. This key should also be mounted in the docker.
- **HADOOP_SSH_PASS** : In case of not having an ssh-key, you could also use the password.

## Clean external resources
As this container will connect to an external persistent hadoop cluster, the created HBase tables will need to be deleted manually. All created tables will be prefixed with "opencga-demo"

## Available hadoop flavours:

| Name                                       | Docker Tag                       |
|--------------------------------------------|----------------------------------|
| Hortonworks HDP 3.1 or Azure HDInsight 4.0 | REPLACEME_OPENCGA_VERSION-hdp3.1 |
| Hortonworks HDP 2.6 or Azure HDInsight 3.6 | REPLACEME_OPENCGA_VERSION-hdp2.6 |
| Amazon EMR 6.1                             | REPLACEME_OPENCGA_VERSION-emr6.1 |


## Example
```shell script
docker run -d --name opencga-demo \
    --network=host \
    --env load=true \
    --env HADOOP_SSH_HOST="my-hadoop-cluster-login-node.internal.zone" \
    --env HADOOP_SSH_USER="sshuser" \
    --env HADOOP_SSH_KEY="/opencga/hadoop-ssh-key.pem" \
    -v /path/to/ssh/key/hadoop-ssh-key.pem:/opencga/hadoop-ssh-key.pem \
    opencb/opencga-demo:REPLACEME_OPENCGA_VERSION-hdp3.1
```