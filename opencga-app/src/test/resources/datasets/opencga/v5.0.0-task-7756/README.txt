# Commands executed to generate the dataset

padmin="4dMiNiStR4t0R."
puser="Test_P4ss"
./opencga-admin.sh catalog install --force
echo "$padmin" | ./opencga-admin.sh catalog install --force
echo "$padmin" | ./opencga.sh users login -u opencga -p
vim ../conf/client-configuration.yml
echo "$padmin" | ./opencga.sh users login -u opencga -p
./opencga.sh organizations create --id test
./opencga.sh users create --email demo@opencga.com --id test --name Demo --password $puser --organization test
./opencga.sh organizations update --organization test --owner test
echo "$puser" | ./opencga.sh users login -u test -p

# Create project and study
./opencga.sh projects create --id testProject --name "Test Project" --description "Migration test" --organism-assembly GRCh38 --organism-scientific-name "Homo sapiens"
./opencga.sh studies create --id testStudy --name "Test Study" --project testProject

# Create individuals with different quality control configurations
# Individual 1: With QC data
./opencga.sh individuals create --id ind1 --name "Individual1" --sex-id MALE
echo '{"qualityControl": {"inferredSexReports":[{"method":"CoverageRatio"}], "mendelianErrorReports":[{"numErrors":0}]}}' > ind_qc_update.json
./opencga.sh individuals update --individuals ind1 --json-file ind_qc_update.json
rm ind_qc_update.json

# Individual 2: Without QC data
./opencga.sh individuals create --id ind2 --name "Individual2" --sex-id FEMALE

# Create families with different quality control configurations
# Family 1: With QC data
./opencga.sh individuals create --id father --name "Father" --sex-id MALE
./opencga.sh individuals create --id mother --name "Mother" --sex-id FEMALE
./opencga.sh individuals create --id child --name "Child" --sex-id MALE --father-id father --mother-id mother
./opencga.sh families create --id fam1 --name "Family1" --members father,mother,child
echo '{"qualityControl": {"relatedness":[{"method":"PLINK/IBD"}]}}' > fam_qc_update.json
./opencga.sh families update --families fam1 --json-file fam_qc_update.json
rm fam_qc_update.json

# Family 2: Without QC data
./opencga.sh families create --id fam2 --name "Family2"

# Create samples with different quality control configurations
# Sample 1: With variant QC data
./opencga.sh samples create --id sample1 --individual-id ind1
echo '{"qualityControl": {"variant":{"variantStats":[{"id":"stat1"}],"signatures":[{"id":"sig1"}],"genomePlot":{"id": "plot1"}}}}' > sam_qc_update.json
./opencga.sh samples update --samples sample1 --json-file sam_qc_update.json

# Sample 2: With files QC data
./opencga.sh samples create --id sample2 --individual-id ind1
echo '{"qualityControl": {"files": ["file1"]}}' > sam_qc_update.json
./opencga.sh samples update --samples sample2 --json-file sam_qc_update.json
rm sam_qc_update.json

# Sample 3: Without QC data
./opencga.sh samples create --id sample3 --individual-id ind2

# Create clinical analysis with different cvdbIndex configurations
# Clinical Analysis 1: With cvdbIndex
./opencga.sh clinical create --id case1 --type SINGLE --proband-id ind1