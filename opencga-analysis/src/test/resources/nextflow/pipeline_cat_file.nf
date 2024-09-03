#!/usr/bin/env nextflow

params.in = "$baseDir/myfile.txt"

process foo {
  output:
  stdout

  script:
  """
  echo `cat $params.in` world > file.out
  """
}

/*
 * Define the workflow
 */
workflow {
    foo
}