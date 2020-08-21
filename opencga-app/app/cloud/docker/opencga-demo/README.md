# OpenCGA DEMO

A standalone image with a complete opencga installation for demo and testing purposes.
Not for production usage.

Includes an embedded MongoDB and Solr.

You can optionally load some demo data. Launch the container with the env var `load=true` to load data from the [Corpasome study](https://figshare.com/articles/Corpasome/693052). This process takes from 30 to 45 minutes.

```
docker run -d --name opencga-demo --env load=true -p 9090:9090 opencb/opencga-demo:2.0.0-rc1
```


## Interact with the container
Creating a convenient alias
```
alias opencga.sh='docker exec -it opencga-demo ./opencga.sh'
```

Then you can login (user: `demo`, password: `demo`) and check the progress of the data load.
```
opencga.sh users login -u demo
# Password: demo

opencga.sh jobs top
```

After 30 minutes the top should look like this:
![](img/jobs_top.png)

## Connect with IVA
You could also start an IVA to connect with this opencga-demo.

```
docker run -d --name iva-app -p 80:80 opencb/iva-app:2.0.0-beta
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


