#!/bin/bash

# Check if the required parameters are provided
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
  echo "Usage: $0 <input dir> <VCF base> filename>"
  echo "  <input dir>:    The input directory."
  echo "  <VCF filename>: The filename of the input VCF file."
  echo "  <output basename>: The basename of the output files."
  exit 1
fi

# Assign the input parameters to variables
input_dir="$1"
base="${2%.vcf*}"
output_basename="$3"

# Add a trailing slash to dir if it's not already there.
if [[ "$dir" != */ ]]; then
    dir="$dir/"
fi

echo "Running with input dir: $input_dir, base: $base and output basename: $output_basename"

# 1. bcftools annotate
echo "1. Annotating VCF using bcftools..."
bcftools annotate --set-id '%CHROM:%POS:%REF:%FIRST_ALT' "${input_dir}/$2" -Oz > "${input_dir}/${base}.annotated.vcf.gz"
if [ $? -ne 0 ]; then
  echo "Error: bcftools annotate failed!"
  exit 1
fi

# 2. plink --make-bed
echo "2. Running plink --make-bed..."
plink1.9 --vcf "${input_dir}/${base}.annotated.vcf.gz" --make-bed --out "${input_dir}/${base}.annotated" --memory 30600
if [ $? -ne 0 ]; then
  echo "Error: plink --make-bed failed!"
  exit 1
fi

# 3. plink --indep-pairwise
echo "3. Running plink --indep-pairwise..."
#plink1.9 --bfile "${input_dir}/${base}.annotated" --indep-pairwise 50 5 0.2 --maf 0.05 --out "${input_dir}/variants" --memory 30600
plink1.9 --bfile "${input_dir}/${base}.annotated" --indep-pairwise 50 5 0.2 --out "${input_dir}/variants" --memory 30600
if [ $? -ne 0 ]; then
  echo "Error: plink --indep-pairwise failed!"
  exit 1
fi

# 4. plink --extract
echo "4. Running plink --extract..."
plink1.9 --bfile "${input_dir}/${base}.annotated" --extract "${input_dir}/variants.prune.in" --make-bed --out "${input_dir}/${output_basename}" --memory 30600
if [ $? -ne 0 ]; then
  echo "Error: plink --extract failed!"
  exit 1
fi

#5. Delete intermediate files
echo "6. Deleting intermediate files..."
rm -v "${input_dir}/${base}.annotated.vcf.gz"
rm -v "${input_dir}/${base}.annotated.bed" "${input_dir}/${base}.annotated.bim" "${input_dir}/${base}.annotated.fam" "${input_dir}/${base}.annotated.nosex" "${input_dir}/${base}.annotated.log"
rm -v "${input_dir}/variants.prune.in" "${input_dir}/variants.prune.out" "${input_dir}/variants.nosex" "${input_dir}/variants.log"

echo "All steps completed successfully!"
