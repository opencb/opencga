# JavaScript

### Fetching the first 10 samples of the defined Study 

```text
import {OpenCGAClient} from "./opencga-client.js";
import {RestResponse} from "./../rest-response.js";

const host = "";      // add your host
const study = "";     // add your study
const token = "";     // add a valid token

const client = new OpenCGAClient({
    host: host,
    version: "v2",
    cookies: {active: false},
    token: token
});


(async () => {
    try {
        const restResponse = await client.samples().search({limit:10, study: study});
        console.table(restResponse.getResults());
    } catch (response) {
        if (response instanceof RestResponse) {
            console.error(response.getEvents())
        } else {
            console.error(response)
        }
    }
})()
```

