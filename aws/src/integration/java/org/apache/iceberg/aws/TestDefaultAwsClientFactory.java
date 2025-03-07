/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestDefaultAwsClientFactory {

  @Test
  public void testS3FileIoEndpointOverride() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.S3FILEIO_ENDPOINT, "https://unknown:1234");
    AwsClientFactory factory = AwsClientFactories.from(properties);
    S3Client s3Client = factory.s3();
    AssertHelpers.assertThrowsCause("Should refuse connection to unknown endpoint",
        SdkClientException.class,
        "Unable to execute HTTP request: unknown",
        () -> s3Client.getObject(GetObjectRequest.builder().bucket("bucket").key("key").build()));
  }

  @Test
  public void testS3FileIoCredentialsOverride() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.S3FILEIO_ACCESS_KEY_ID, "unknown");
    properties.put(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, "unknown");
    AwsClientFactory factory = AwsClientFactories.from(properties);
    S3Client s3Client = factory.s3();
    AssertHelpers.assertThrows("Should fail request because of bad access key",
        S3Exception.class,
        "The AWS Access Key Id you provided does not exist in our records",
        () -> s3Client.getObject(GetObjectRequest.builder().bucket("bucket").key("key").build()));
  }
}
