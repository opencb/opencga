## TF Deployment Docs


## FAQ

#### Log Analytics SKU error 

I see the following error during deployment: 

```
* module.loganalytics.azurerm_log_analytics_workspace.opencga: 1 error(s) occurred:

* azurerm_log_analytics_workspace.opencga: operationalinsights.WorkspacesClient#CreateOrUpdate: Failure sending request: StatusCode=400 -- Original Error: Code="BadRequest" Message="Pricing tier doesn't match the subscription's billing model. Read http://aka.ms/PricingTierWarning for more details."
```

This is likely because you have previously deployed a `Log Analytics` workspace with the old pricing model. 

https://docs.microsoft.com/en-gb/azure/azure-monitor/platform/usage-estimated-costs#moving-to-the-new-pricing-model