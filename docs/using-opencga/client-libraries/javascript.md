# JavaScript

The OpenCGA JavaScript Client is provided as part of [JSorolla](https://github.com/opencb/jsorolla).

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

#### Fetching the first 10 variants of the Study of interest using OpenCGA credentials

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
        const restResponse = await session.opencgaClient.variants().query({"study": SUDY, limit: 10});
        console.log(restResponse.getResults());
    } catch (e) {
        console.error(e)
    }
})();
```

