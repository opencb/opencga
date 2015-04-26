/*
 * Copyright 2015 OpenCB
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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.InputStream;

public class S3Source implements Source {

    public InputStream getInputStream(String path) {
        String ak = "AKIAI3BZQ2VG6GPWQBVA";
        String sk = "oDDIv+OAQeQVj9sy1CcWeeJsOMAhbh9KIpJ7hiDK";
        String bucket = "nacho-s3";
        AWSCredentials myCredentials = new BasicAWSCredentials(ak, sk);
        AmazonS3Client s3Client = new AmazonS3Client(myCredentials);

        S3Object object = s3Client.getObject(new GetObjectRequest(bucket, path));

        return object.getObjectContent();
    }

}
