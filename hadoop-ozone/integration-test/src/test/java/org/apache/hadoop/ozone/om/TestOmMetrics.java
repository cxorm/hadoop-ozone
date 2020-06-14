/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Test for OM metrics.
 */
public class TestOmMetrics {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private MiniOzoneCluster cluster;
  private OzoneManager ozoneManager;

  /**
   * The exception used for testing failure metrics.
   */
  private IOException exception = new IOException();

  /**
   * Create a MiniDFSCluster for testing.
   */
  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBucketOps() throws IOException {
    BucketManager bucketManager =
        (BucketManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "bucketManager");
    BucketManager mockBm = Mockito.spy(bucketManager);

    Mockito.doNothing().when(mockBm).createBucket(null);
    Mockito.doNothing().when(mockBm).deleteBucket(null, null);
    Mockito.doReturn(null).when(mockBm).getBucketInfo(null, null);
    Mockito.doNothing().when(mockBm).setBucketProperty(null);
    Mockito.doReturn(null).when(mockBm).listBuckets(null, null, null, 0);

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);

    doBucketOps();

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 5L, omMetrics);
    assertCounter("NumBucketCreates", 1L, omMetrics);
    assertCounter("NumBucketUpdates", 1L, omMetrics);
    assertCounter("NumBucketInfos", 1L, omMetrics);
    assertCounter("NumBucketDeletes", 1L, omMetrics);
    assertCounter("NumBucketLists", 1L, omMetrics);
    assertCounter("NumBuckets", 0L, omMetrics);

    ozoneManager.createBucket(null);
    ozoneManager.createBucket(null);
    ozoneManager.createBucket(null);
    ozoneManager.deleteBucket(null, null);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBuckets", 2L, omMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockBm).createBucket(null);
    Mockito.doThrow(exception).when(mockBm).deleteBucket(null, null);
    Mockito.doThrow(exception).when(mockBm).getBucketInfo(null, null);
    Mockito.doThrow(exception).when(mockBm).setBucketProperty(null);
    Mockito.doThrow(exception).when(mockBm).listBuckets(null, null, null, 0);

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);
    doBucketOps();

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 14L, omMetrics);
    assertCounter("NumBucketCreates", 5L, omMetrics);
    assertCounter("NumBucketUpdates", 2L, omMetrics);
    assertCounter("NumBucketInfos", 2L, omMetrics);
    assertCounter("NumBucketDeletes", 3L, omMetrics);
    assertCounter("NumBucketLists", 2L, omMetrics);

    assertCounter("NumBucketCreateFails", 1L, omMetrics);
    assertCounter("NumBucketUpdateFails", 1L, omMetrics);
    assertCounter("NumBucketInfoFails", 1L, omMetrics);
    assertCounter("NumBucketDeleteFails", 1L, omMetrics);
    assertCounter("NumBucketListFails", 1L, omMetrics);

    assertCounter("NumBuckets", 2L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumBuckets", 2L, omMetrics);
  }

  @Test
  public void testKeyOps() throws IOException {
    KeyManager keyManager = (KeyManager) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    KeyManager mockKm = Mockito.spy(keyManager);
    BucketManager mockBm = Mockito.mock(BucketManager.class);

    OmBucketInfo mockBucket = OmBucketInfo.newBuilder()
        .setVolumeName("").setBucketName("")
        .build();
    Mockito.when(mockBm.getBucketInfo(any(), any())).thenReturn(mockBucket);
    Mockito.doReturn(null).when(mockKm).openKey(any());
    Mockito.doNothing().when(mockKm).deleteKey(any());
    Mockito.doReturn(null).when(mockKm).lookupKey(any(), any());
    Mockito.doReturn(null).when(mockKm).listKeys(any(), any(), any(), any(),
        anyInt());
    Mockito.doReturn(null).when(mockKm).listTrash(any(), any(), any(), any(),
        anyInt());
    Mockito.doNothing().when(mockKm).commitKey(any(), anyLong());
    Mockito.doReturn(null).when(mockKm).initiateMultipartUpload(any());

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);
    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "keyManager", mockKm);
    OmKeyArgs keyArgs = createKeyArgs();
    doKeyOps(keyArgs);

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 7L, omMetrics);
    assertCounter("NumKeyAllocate", 1L, omMetrics);
    assertCounter("NumKeyLookup", 1L, omMetrics);
    assertCounter("NumKeyDeletes", 1L, omMetrics);
    assertCounter("NumKeyLists", 1L, omMetrics);
    assertCounter("NumTrashKeyLists", 1L, omMetrics);
    assertCounter("NumKeys", 0L, omMetrics);
    assertCounter("NumInitiateMultipartUploads", 1L, omMetrics);


    ozoneManager.openKey(keyArgs);
    ozoneManager.commitKey(keyArgs, 0);
    ozoneManager.openKey(keyArgs);
    ozoneManager.commitKey(keyArgs, 0);
    ozoneManager.openKey(keyArgs);
    ozoneManager.commitKey(keyArgs, 0);
    ozoneManager.deleteKey(keyArgs);


    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 2L, omMetrics);

    // inject exception to test for Failure Metrics
    Mockito.doThrow(exception).when(mockKm).openKey(any());
    Mockito.doThrow(exception).when(mockKm).deleteKey(any());
    Mockito.doThrow(exception).when(mockKm).lookupKey(any(), any());
    Mockito.doThrow(exception).when(mockKm).listKeys(
        any(), any(), any(), any(), anyInt());
    Mockito.doThrow(exception).when(mockKm).listTrash(
        any(), any(), any(), any(), anyInt());
    Mockito.doThrow(exception).when(mockKm).commitKey(any(), anyLong());
    Mockito.doThrow(exception).when(mockKm).initiateMultipartUpload(any());

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "keyManager", mockKm);
    doKeyOps(keyArgs);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 21L, omMetrics);
    assertCounter("NumKeyAllocate", 5L, omMetrics);
    assertCounter("NumKeyLookup", 2L, omMetrics);
    assertCounter("NumKeyDeletes", 3L, omMetrics);
    assertCounter("NumKeyLists", 2L, omMetrics);
    assertCounter("NumTrashKeyLists", 2L, omMetrics);
    assertCounter("NumInitiateMultipartUploads", 2L, omMetrics);

    assertCounter("NumKeyAllocateFails", 1L, omMetrics);
    assertCounter("NumKeyLookupFails", 1L, omMetrics);
    assertCounter("NumKeyDeleteFails", 1L, omMetrics);
    assertCounter("NumKeyListFails", 1L, omMetrics);
    assertCounter("NumTrashKeyListFails", 1L, omMetrics);
    assertCounter("NumInitiateMultipartUploadFails", 1L, omMetrics);


    assertCounter("NumKeys", 2L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumKeys", 2L, omMetrics);

  }

  /**
   * Test bucket operations with ignoring thrown exception.
   */
  private void doBucketOps() {
    try {
      ozoneManager.createBucket(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.deleteBucket(null, null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.getBucketInfo(null, null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.setBucketProperty(null);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listBuckets(null, null, null, 0);
    } catch (IOException ignored) {
    }
  }

  /**
   * Test key operations with ignoring thrown exception.
   */
  private void doKeyOps(OmKeyArgs keyArgs) {
    try {
      ozoneManager.openKey(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.deleteKey(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.lookupKey(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listKeys("", "", null, null, 0);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listTrash("", "", null, null, 0);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.commitKey(keyArgs, 0);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.initiateMultipartUpload(keyArgs);
    } catch (IOException ignored) {
    }

  }

  private OmKeyArgs createKeyArgs() {
    OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(new ContainerBlockID(1, 1)))
        .build();
    keyLocationInfo.setCreateVersion(0);

    return new OmKeyArgs.Builder()
        .setLocationInfoList(Collections.singletonList(keyLocationInfo))
        .build();
  }
}
