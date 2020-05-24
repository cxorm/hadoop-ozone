/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell.keys;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;

/**
 * Recover deleted key to destination bucket.
 */
@Command(name = "recover-trash",
    description = "Recover the deleted key to destination bucket")
public class RecoverKeyHandler extends KeyHandler {

  @Parameters(index = "1", arity = "1..1",
      description = "Destination bucket to recover the key to")
  private String destBucketName;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);

    /* TODO: Check the bucket is trash-enable or not, */

    /* TODO: Check the destination is existed or not. */
    OzoneBucket destinationBucket = vol.getBucket(destBucketName);

    System.out.println("Original Bucket: " + bucket.getName());
    System.out.println("Destination Bucket: " + destinationBucket.getName());

    if (bucket.recoverTrash(keyName, destBucketName)) {
      System.out.println("recover successfully");
    } else {
      System.out.println("recover fail");
    }

      /* If recovering trash to existing bucket*/
    try {
      bucket.recoverTrash(keyName, destBucketName);
    } catch (IOException bucketNotFoundExp) {
      System.out.println("Exp: " + bucketNotFoundExp.getMessage());
      String bucketExp = bucketNotFoundExp.getMessage();

      System.out.println("cause: " + BUCKET_NOT_FOUND.name());
      if (bucketExp.equals(BUCKET_NOT_FOUND.name())) {
        /* */
        System.out.println("hit!!!!!!");
      }
    }

    /* If recovering trash to non existing bucket*/

  }
}
