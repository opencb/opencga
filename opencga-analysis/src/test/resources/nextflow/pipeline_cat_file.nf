#!/usr/bin/env nextflow

params.in = "$baseDir/myfile.txt"

process foo {
  output:
  stdout

  script:
  """
  echo `cat $params.in` world > /data/output/file.out
  """
}

/*
 * Define the workflow
 */
workflow {
    foo
}