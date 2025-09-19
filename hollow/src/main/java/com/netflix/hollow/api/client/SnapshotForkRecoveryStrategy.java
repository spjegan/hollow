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
import com.netflix.hollow.core.HollowConstants;
import java.util.logging.Logger;

/**
 * Default recovery strategy that uses snapshot-based recovery.
 * This strategy represents the existing behavior of falling back to snapshot plans
 * when delta plans cannot reach the desired version.
 */
public class SnapshotForkRecoveryStrategy implements DeltaForkRecoveryStrategy {
    private static final Logger LOG = Logger.getLogger(SnapshotForkRecoveryStrategy.class.getName());

    @Override
    public HollowUpdatePlan createRecoveryPlan(
            long currentVersion,
            HollowConsumer.VersionInfo desiredVersionInfo,
            HollowConsumer.BlobRetriever blobRetriever,
            HollowConsumer.DoubleSnapshotConfig doubleSnapshotConfig,
            HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) throws Exception {
        
        LOG.info("Attempting snapshot-based recovery for version transition: " 
                + currentVersion + " -> " + desiredVersionInfo.getVersion());
        
        return createSnapshotPlan(desiredVersionInfo, blobRetriever, updatePlanBlobVerifier);
    }

    /**
     * Creates a snapshot-based update plan.
     * This method replicates the logic from HollowUpdatePlanner.snapshotPlan().
     */
    private HollowUpdatePlan createSnapshotPlan(
            HollowConsumer.VersionInfo desiredVersionInfo,
            HollowConsumer.BlobRetriever blobRetriever,
            HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) {
        
        HollowUpdatePlan plan = new HollowUpdatePlan();
        long desiredVersion = desiredVersionInfo.getVersion();
        long nearestPreviousSnapshotVersion = includeNearestSnapshot(plan, desiredVersionInfo, blobRetriever, updatePlanBlobVerifier);

        // The includeNearestSnapshot function returns a snapshot version that is less than or equal to the desired version
        if(nearestPreviousSnapshotVersion > desiredVersion)
            return HollowUpdatePlan.DO_NOTHING;

        // If the nearest snapshot version is HollowConstants.VERSION_LATEST then no past snapshots were found
        if(nearestPreviousSnapshotVersion == HollowConstants.VERSION_LATEST)
            return HollowUpdatePlan.DO_NOTHING;

        plan.appendPlan(createDeltaPlan(nearestPreviousSnapshotVersion, desiredVersion, Integer.MAX_VALUE, blobRetriever));

        return plan;
    }

    /**
     * Creates a delta plan from current version to desired version.
     * This method replicates the logic from HollowUpdatePlanner.deltaPlan().
     */
    private HollowUpdatePlan createDeltaPlan(long currentVersion, long desiredVersion, int maxDeltas, HollowConsumer.BlobRetriever blobRetriever) {
        HollowUpdatePlan plan = new HollowUpdatePlan();
        if(currentVersion < desiredVersion) {
            applyForwardDeltasToPlan(currentVersion, desiredVersion, plan, maxDeltas, blobRetriever);
        } else if(currentVersion > desiredVersion) {
            applyReverseDeltasToPlan(currentVersion, desiredVersion, plan, maxDeltas, blobRetriever);
        }
        return plan;
    }

    private long applyForwardDeltasToPlan(long currentVersion, long desiredVersion, HollowUpdatePlan plan, int maxDeltas, HollowConsumer.BlobRetriever blobRetriever) {
        int transitionCounter = 0;

        while(currentVersion < desiredVersion && transitionCounter < maxDeltas) {
            currentVersion = includeNextDelta(plan, currentVersion, desiredVersion, blobRetriever);
            transitionCounter++;
        }
        return currentVersion;
    }

    private long applyReverseDeltasToPlan(long currentVersion, long desiredVersion, HollowUpdatePlan plan, int maxDeltas, HollowConsumer.BlobRetriever blobRetriever) {
        long achievedVersion = currentVersion;
        int transitionCounter = 0;

        while (currentVersion > desiredVersion && transitionCounter < maxDeltas) {
            currentVersion = includeNextReverseDelta(plan, currentVersion, blobRetriever);
            if (currentVersion != HollowConstants.VERSION_NONE)
                achievedVersion = currentVersion;
            transitionCounter++;
        }

        return achievedVersion;
    }

    /**
     * Includes the next delta only if it will not take us *after* the desired version
     */
    private long includeNextDelta(HollowUpdatePlan plan, long currentVersion, long desiredVersion, HollowConsumer.BlobRetriever blobRetriever) {
        HollowConsumer.Blob transition = blobRetriever.retrieveDeltaBlob(currentVersion);

        if(transition != null) {
            if(transition.getToVersion() <= desiredVersion) {
                plan.add(transition);
            }

            return transition.getToVersion();
        }

        return HollowConstants.VERSION_LATEST;
    }

    private long includeNextReverseDelta(HollowUpdatePlan plan, long currentVersion, HollowConsumer.BlobRetriever blobRetriever) {
        HollowConsumer.Blob transition = blobRetriever.retrieveReverseDeltaBlob(currentVersion);
        if(transition != null) {
            plan.add(transition);
            return transition.getToVersion();
        }

        return HollowConstants.VERSION_NONE;
    }

    /**
     * This method replicates the logic from HollowUpdatePlanner.includeNearestSnapshot().
     */
    private long includeNearestSnapshot(HollowUpdatePlan plan, HollowConsumer.VersionInfo desiredVersionInfo, 
                                       HollowConsumer.BlobRetriever blobRetriever, HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) {
        long desiredVersion = desiredVersionInfo.getVersion();
        HollowConsumer.Blob transition = blobRetriever.retrieveSnapshotBlob(desiredVersion);
        if (transition != null) {
            // exact match with desired version
            if (transition.getToVersion() == desiredVersion) {
                plan.add(transition);
                return transition.getToVersion();
            }

            // else if update is to an announced version then add only announced versions to plan
            if (updatePlanBlobVerifier != null
                && updatePlanBlobVerifier.announcementVerificationEnabled()
                && desiredVersionInfo.wasAnnounced() != null
                && desiredVersionInfo.wasAnnounced().isPresent()
                && desiredVersionInfo.wasAnnounced().get()) {

                int lookback = 1;
                int maxLookback = updatePlanBlobVerifier.announcementVerificationMaxLookback();
                HollowConsumer.AnnouncementWatcher announcementWatcher = updatePlanBlobVerifier.announcementWatcher();
                while (lookback <= maxLookback) {
                    HollowConsumer.AnnouncementStatus announcementStatus = announcementWatcher == null
                            ? HollowConsumer.AnnouncementStatus.UNKNOWN : announcementWatcher.getVersionAnnouncementStatus(transition.getToVersion());

                    if (announcementStatus == null
                     || announcementStatus.equals(HollowConsumer.AnnouncementStatus.UNKNOWN)
                     || announcementStatus.equals(HollowConsumer.AnnouncementStatus.ANNOUNCED)) {
                        // backwards compatibility
                        if (announcementStatus == HollowConsumer.AnnouncementStatus.UNKNOWN) {
                            if (announcementWatcher == null) {
                                LOG.warning("HollowUpdatePlanner was not initialized with an announcement watcher so it does not support getVersionAnnouncementStatus. " +
                                        "Consumer will continue with the update but runs the risk of consuming a snapshot version that was not announced");
                            } else {
                                LOG.warning(String.format("Announcement watcher impl bound (%s) to HollowUpdatePlanner does not support getVersionAnnouncementStatus. " +
                                        "Consumer will continue with the update but runs the risk of consuming a snapshot version that was not announced", announcementWatcher.getClass().getName()));
                            }
                        } else if (announcementStatus == null) {
                            LOG.warning(String.format("Expecting a valid announcement stats for version(%s), but Announcement watcher impl (%s) " +
                                    "returned null", transition.getToVersion(), announcementWatcher.getClass().getName()));
                        }
                        plan.add(transition);
                        return transition.getToVersion();
                    }
                    lookback ++;
                    desiredVersion = transition.getToVersion() - 1; // try the next highest snapshot version less than the previous one
                    transition = blobRetriever.retrieveSnapshotBlob(desiredVersion);
                    if (transition == null) {
                        break;
                    }
                }
                LOG.warning("No past snapshot found within lookback period that corresponded to an announced version, maxLookback configured to " + maxLookback);
            } else {
                // if desired version is either not an announced version, or unknown whether it is an announced version (e.g. backwards compatibility)
                plan.add(transition);
                return transition.getToVersion();
            }
        }
        return HollowConstants.VERSION_LATEST;
    }
}
