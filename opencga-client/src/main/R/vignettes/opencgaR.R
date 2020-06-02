## ---- eval=FALSE--------------------------------------------------------------
#  ## Initialise connection specifying host and REST version
#  con <- initOpencgaR(host = "http://bioinfo.hpc.cam.ac.uk/opencga-prod/", version = "v2")
#  
#  ## Initialise connection using a configuration in R list
#  conf <- list(version="v2", rest=list(host="http://bioinfo.hpc.cam.ac.uk/opencga-prod/"))
#  con <- initOpencgaR(opencgaConfig=conf)
#  
#  ## Initialise connection using a configuration file (in YAML or JSON format)
#  conf <- "/path/to/conf/client-configuration.yml"
#  con <- initOpencgaR(opencgaConfig=conf)

## ---- eval=FALSE--------------------------------------------------------------
#  # Log in
#  con <- opencgaLogin(opencga = con, userid = "demouser", passwd = "demouser")

