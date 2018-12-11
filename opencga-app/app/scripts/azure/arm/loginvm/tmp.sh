az vm create \
  --resource-group Demo-OpenCGADev \
  --name marcuscloudinittest2 \
  --image Canonical:UbuntuServer:18.04-LTS:latest \
  --custom-data /mnt/c/repos/opencga-marrobi/opencga-app/app/scripts/azure/arm/loginvm/cloudinit.yaml \
    --admin-password $PASSWORD      \
    --admin-username opencgaadmin