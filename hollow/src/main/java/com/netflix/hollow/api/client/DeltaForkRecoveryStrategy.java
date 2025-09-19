/*
 *  Copyright 2016-2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.api.client;

import com.netflix.hollow.api.consumer.HollowConsumer;

/**
 * Strategy interface for delta fork recovery mechanisms.
 * Defines how to create an update plan when the direct delta path cannot reach the desired version.
 */
public interface DeltaForkRecoveryStrategy {
    
    /**
     * Attempts to create an update plan to reach the desired version when the standard delta plan fails.
     * 
     * @param currentVersion The current version of the hollow state engine
     * @param desiredVersionInfo The version info to which the hollow state engine should be updated
     * @param blobRetriever The blob retriever for accessing snapshot and delta blobs
     * @param doubleSnapshotConfig Configuration for double snapshot behavior
     * @param updatePlanBlobVerifier Update plan verifier for blob validation
     * @return A recovery update plan, or null if recovery is not possible
     * @throws Exception if the recovery plan cannot be created
     */
    HollowUpdatePlan createRecoveryPlan(
            long currentVersion,
            HollowConsumer.VersionInfo desiredVersionInfo,
            HollowConsumer.BlobRetriever blobRetriever,
            HollowConsumer.DoubleSnapshotConfig doubleSnapshotConfig,
            HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) throws Exception;
}
