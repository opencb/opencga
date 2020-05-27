# OpenCGA on Azure

You can choose to either use a manual single machine setup or an automated HA environment setup. 

For [manual setup follow this guide](manualsetup.md) and for an [automated setup follow this guide](arm/README.md).

Once completed you can use the guide below to test your install with a sample process.

## Testing
Once everything is successfully setup, you are able to test the configuration. A basic test scenario is decribed below. For full details check out the [OpenCGA documentation](http://docs.opencb.org/display/opencga/Getting+Started+in+5+minutes).

> Note: If you're running in the Azure ARM version you don't need to start the daemon or install catalog, they will have already been started. The Daemon will already be running in Docker on the Daemon node. Run `sudo docker ps` and note the ID of the container then use `sudo docker exec -it <IDHERE> /bin/bash` to start an interactive shell for use below and skip the next two steps. 

Install catalog (remember password for later use)
```
sudo /opt/opencga/bin/opencga-admin.sh catalog install
```

Start daemon (for debugging) in a seperate terminal and keep it open
```
sudo /opt/opencga/bin/opencga-admin.sh catalog daemon --start
```

Create a new user (in a different terminal)
```
sudo /opt/opencga/bin/opencga-admin.sh users create -u bart --email test@gel.ac.uk --name "John Doe" --user-password testpwd
```

Login to get a session token (replace bart with your newly created user)
```
sudo /opt/opencga/bin/opencga.sh users login -u bart
```

Create sample project
```
sudo /opt/opencga/bin/opencga.sh projects create --id reference_grch37 -n "Reference studies GRCh37" --organism-scientific-name "Homo sapiens" --organism-assembly "GRCh37"
```

If you run into a permissions error, make sure to change the permissions on the `/opt/opencga` folder once again
```
sudo chmod -R 777 /opt/opencga/
```

Create sample study within your project
```
sudo /opt/opencga/bin/opencga.sh studies create --id 1kG_phase3 -n "1000 Genomes Project - Phase 3" --project reference_grch37
```

> Note: If you're running in the Azure ARM version you need to download and link the file from a shared location like `/opt/opencga/sessions`. Run `cd /opt/opencga/sessions` and then continue from the guide in that directory. 

Get sample VCF genome file
```
wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
```

Link VCF genome file to your newly created study
```
sudo /opt/opencga/bin/opencga.sh files link -i ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz -s 1kG_phase3
```

### Integrated pipeline load (Without Solr search index)

> Note: This performs `transform`, `load`, `calculateStats` and `annotate` all in one pipeline which simplifies execution. 

```
sudo /opt/opencga/bin/opencga.sh variant index --file ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz --calculate-stats --annotate -o outDir
```

### Integrated pipeline load (Including load into Solr search index)

```
sudo /opt/opencga/bin/opencga.sh variant index --file ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz --calculate-stats --annotate --index-search -o outDir
```

### Separate pipeline load (Advanced/Manual)

> Note: This performs `transform`, `load`, `calculateStats` and `annotate` individually and isn't recommended for normal usage. 

Transform file (view progress in separate daemon terminal)
```
sudo /opt/opencga/bin/opencga.sh variant index --file ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz --transform -o outDir
```

Load file (view progress in separate daemon terminal), won't start until previous transform step is completed
```
sudo /opt/opencga/bin/opencga.sh variant index --file ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz --load -o outDir
```

Perform query on successfully indexed file
```
sudo /opt/opencga/bin/opencga-analysis.sh variant query --sample HG00096 --limit 100
```

### Testing with Platinum Data set

```
for i in $( seq 77 93 )
do
        wget http://bioinfo.hpc.cam.ac.uk/downloads/datasets/vcf/platinum_genomes/gz/platinum-genomes-vcf-NA128"$i"_S1.genome.vcf.gz
done

for i in $( seq 77 93 )
do
        /opt/opencga/bin/opencga.sh files link -i platinum-genomes-vcf-NA128"$i"_S1.genome.vcf.gz -s 1kG_phase3 &&
        /opt/opencga/bin//opencga.sh variant index --file platinum-genomes-vcf-NA128"$i"_S1.genome.vcf.gz --calculate-stats --annotate -o tmplsls -s 1kG_phase3 --index-search
done
```


