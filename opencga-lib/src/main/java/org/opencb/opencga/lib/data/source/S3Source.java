package org.opencb.opencga.lib.data.source;

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
