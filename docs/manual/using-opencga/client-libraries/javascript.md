# JavaScript

The OpenCGA JavaScript Client is provided as part of [JSorolla](https://github.com/opencb/jsorolla).  
Some examples of basic usage can be found in [examples](https://github.com/opencb/jsorolla/tree/develop/src/core/clients/opencga/examples) directory.

### Example:

#### Fetching the first 10 variants of the Study of interest using a token

```text
import {OpenCGAClient} from "./opencga-client.js";

const HOST = "";      // add your host
const STUDY = "";     // add your study of interest
const TOKEN = "";     // add a valid token

const client = new OpenCGAClient({
    host: HOST,
    version: "v2",
    cookies: {active: false},
    token: TOKEN
});


(async () => {
    try {
        const restResponse = await client.variants().query({study: STUDY, limit:10});
        console.table(restResponse.getResults());
    } catch (response) {
        if (response instanceof RestResponse) {
            console.error(response.getEvents())
        } else {
            console.error(response)
        }
    }
})();
```

#### Fetching the first 10 variants of the Study of interest using OpenCGA credentials.

In this case an Opencga Session is created. The Opencga Study being used is the default one for the user. 

```text
import {OpenCGAClient} from "./opencga-client.js";

const HOST = "";      // add your host
const STUDY = "";     // add your study of interest
const USERNAME = "";     // add your username
const PASSWORD = "";     // add your username

const client = new OpenCGAClient({
    host: HOST,
    version: "v2",
    cookies: {active: false}
});
(async () => {
    try {
        await client.login(USERNAME, PASSWORD)
        const session = await client.createSession();
        const restResponse = await session.opencgaClient.variants().query({limit:10, study: session.study.fqn});
        console.table(restResponse.getResults());

    } catch (e) {
        console.error(e)
    }
})();
```

