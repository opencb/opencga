# Configuration

## **How to configure the OpenCGA command line for my installation?**

OpenCGA offers various ways to interact with the variant data stored in the project/studies, including the IVA web application, R, Python and Java clients libraries and a powerful Command line interface \(CLI\).

This document describes how to download, configure and execute the OpenCGA CLI assuming that:

* You have a Linux or Mac work station with internet access and a Java Runtime installed. This tutorial is performed using Ubuntu 20.04.2 LTS. If you have problems installing OpenCGA CLI on other platforms, please contact at support@zettagenomics.com  or create a ticket at[ Zetta Service Desk](https://zettagenomics.atlassian.net/servicedesk/customer/portal/1).
* You have access to an OpenCGA server. This tutorial uses the public server at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/). 
* You already have a user. This tutorial uses the readonly demouser:demouser user. Having a user with admin credentials will allow you to perform more advanced tasks like populating metadata, providing permissions or ingesting new datasets.

## **Procedure**

### **1. Download the OpenCGA from the GitHub official repository**

In a web browser navigate to the official OpenCGA GitHub repository at [https://github.com/opencb/opencga](https://github.com/opencb/opencga)/releases. Access the latest release page  by clicking  the release tag at the right side of the main screen shown below:

![](https://lh5.googleusercontent.com/DgRb-6_zQTOYsc081hZgS2LHiIvTcemEo7sm51dKgzSfc9R9iPE1VIV74G5h40EOTvexCb244sGHK2cOy8y7KeugPCMUmigpUZc0xFAGJSC1mQJmvJ33gyHJEtVy3iy4z1LJwl34)

{% hint style="warning" %}
**Note:** [this page](https://github.com/opencb/opencga/releases) lists all of the OpenCGA releases. We recommend using the latest stable version, especially if it’s your first time interacting with OpenCGA and its CLI. Newer rc \(release candidate\) releases may be available but these require compiling which is outside   
the scope of these instructions. 
{% endhint %}

On the release page click the `opencga-client-x.x.x.tar.gz` link, the package download should start automatically:

![](https://lh4.googleusercontent.com/acxlbLZ2ois1d8Y4KtHXXEQAgEr6HJwzKqhgsoWpyZJcsWS7dprN1sCaOedTzMLm15gn_-rZ2FSrC-T_B8reO7PDpyKJnbH6FZRvkRrjlUrteknfyBAZ7rojSi9NnfdI0xi9rXVS)

### **2. Folder organisation:**

Once the file is downloaded, you should be able to find it in your local Download folder:

![](https://lh3.googleusercontent.com/okpuqX7QUSL1SKVq41rewSZvSb_Wta4HMMUEHENgRGbSSLmwAcZ1ryewc5ybYUrp0FqQxfxD63hX-0G4oMLepzseq4UnmzuYk4m6fEVGO6I4IDV41ju2gtgZs4yFBN7H1cLHMQMb)

Unzip and place the folder in a proper location within your file system.  One possible example of organisation could be as follows.

First, move the folder to your desire location: 

```text
$ mv $HOME/Downloads/opencga-client-2.0.3.tar.gz $HOME/
```

Now, decompress the folder containing the OpenCGA CLI using the next command:

```text
$ tar -xvzf $HOME/opencga-client-2.0.3.tar.gz -C $HOME/
```

### **3. CLI configuration:**

Once the folder has been decompressed and placed in a path where you can make proper use of the CLI tool, the next step consists in editing the config file so it points to the URL of the OpenCGA installation in which we’d like to use the CLI.

For that, open the `client-configuration.yml` file in the conf folder with your usual text editor:

```text
$ nano $HOME/opencga-client-2.0.3/conf/client-configuration.yml 
```

To change the host to your OpenCGA installation, go to the host parameter in the REST client configuration options and change the default URL to point to your installation. Save the changes and exit the editor.

**Example:** let’s suppose we’d like to use the command line in the university demo installation at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/). 

![](https://lh3.googleusercontent.com/dwB8DODonXFkljYgMGB3GX2eX8_IZvTXtNWW3A7NjVQWqId8k7JEEIBVYIGHf4S-yIXRFXdFcMRbLTYlkbzZrskBi3IL78hOW-iFLl3tl2HszH0OnQ9FuijjVKxcl5lRmevZcdBC)

### **4. Start using the CLI: opencga.sh**

You can call the command line with the program opencga.sh \(note that for convenience you can add the bin folder in your $PATH variable in bashrc or similar\):

```text
$ $HOME/opencga-client-2.0.3/bin/opencga.sh 
```

![](https://lh5.googleusercontent.com/L4361kOA0KHSBgZrtW1N2__YHXwb-0TlQU8Nutiada2UqeNHlthyaFWbBs1nN_vfn03gpIHgIHjpUUT3RkrPG37P3YCemn2_58bGujobiMeq_sH23yX5k792Kx7LY3mAuVLrnZiz)

To test that the command line works properly, you can try to login into the demo installation with the demouser credentials: **user**: `demouser`, **password**: `demouser`.

```text
$ echo "demouser" | $HOME/opencga-client-2.0.3/bin/opencga.sh users login -u demouser 
```

**`You have been logged in correctly. This is your new token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkZW1vdXNlciIsImF1ZCI6Ik9wZW5DR0EgdXNlcnMiLCJpYXQiOjE2MjQzMDg2NzMsImV4cCI6MTYyNDMxMjI3M30.YVNlvEDdqR02QJr6GLYCCB6WeGs7h8fyrscSsjyBSSM`**

If the login is successful, the token associated with your session will appear on the screen. 

Now you should be ready to start using the CLI for querying, updating and ingesting data in OpenCGA!

