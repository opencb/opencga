# Variant Aggregation Stats

## Sample Variant Stats

Sample Variant Stats will contain a set of aggregated statistics values for each sample contained in a study, or for a set of samples defined by the user.

```text
./opencga.sh variant sample-stats-run --study <STUDY> --sample all
```

These aggregated values can be computed across all variants from each sample, or using a subset of variants using a variant filter query. e.g:

```text
./opencga.sh variant sample-stats-run --study <STUDY>
                --sample all
                --variant-query ct=missense_variant
                --variant-query biotype=protein_coding
```

By default, this analysis will produce a file, and optionally, the result can be indexed in the Catalog metadata store, given an ID. 

```text
./opencga.sh variant sample-stats-run --study <STUDY>
                --sample all
                --index
                --index-id missense_variants
                --variant-query ct=missense_variant
                --variant-query biotype=protein_coding
```

The ID ALL can only be used if without any variant query filter.

```text
./opencga.sh variant sample-stats-run --study <STUDY>
                 --sample all
                 --index
                 --index-id ALL
```



