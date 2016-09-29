/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.data.source;

import javax.net.ssl.HttpsURLConnection;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class UrlSource implements Source {

    public InputStream getInputStream(String path) {
        try {
            URL url = new URL(path);
            if ("https".equals(url.getProtocol())) {
                HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
                return con.getInputStream();
            } else {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                return con.getInputStream();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
}
