The default rules in **all** NSGs:

Rule | Source Address | Destination Address | Dest Port
-----|-------------------|------------------------|------
_Default_ Allow Load Balancer | AzureLoadBalancer | * | *
_Default_ Allow Vnet Inbound | VirtualNetwork | VirtualNetwork | *
_Default_ Deny All Inbound | * | * | *

The rules that this template sets up are as follows:

`Default` subnet

Rule | Source Address | Destination Address | Dest Port
-----|-------------------|------------------------|------
Allow Http Inbound | * | * | 80
Allow Https Inbound | * | * | 443
Allow SSH Inbound | * | * | 22

`Mongo` subnet

Rule | Source Address | Destination Address | Dest Port
-----|-------------------|------------------------|------
Allow Http Inbound | * | * | 80

`Solr` subnet

Rule | Source Address | Destination Address | Dest Port
-----|-------------------|------------------------|------

`Avere` subnet

Rule | Source Address | Destination Address | Dest Port
-----|-------------------|------------------------|------

`Batch` subnet

Rule | Source Address | Destination Address | Dest Port
-----|-------------------|------------------------|------


# Open Questions

- Does port `8080` need to be open to the **whole** internet, or is it OK if I just limit it to the LoadBalancer for the Tomcat Health Probe?
- Which ports are exposed on the HDInsight cluster? https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-port-settings-for-services