/*
 *  Copyright 2016-2025 Netflix, Inc.
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

    private final HollowConsumer.BlobRetriever blobRetriever;
    private final HollowDeltaUpdatePlanner deltaUpdatePlanner;
    private static final Logger LOG = Logger.getLogger(SnapshotForkRecoveryStrategy.class.getName());

    public SnapshotForkRecoveryStrategy(HollowConsumer.BlobRetriever blobRetriever) {
        this.blobRetriever = blobRetriever;
        this.deltaUpdatePlanner = new HollowDeltaUpdatePlanner(blobRetriever);
    }

    @Override
    public HollowUpdatePlan createRecoveryPlan(
            long currentVersion,
            HollowConsumer.VersionInfo desiredVersionInfo,
            HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) throws Exception {
        
        LOG.info("Attempting snapshot-based recovery for version transition: " 
                + currentVersion + " -> " + desiredVersionInfo.getVersion());
        
        return snapshotPlan(desiredVersionInfo, updatePlanBlobVerifier);
    }

    /**
     * Returns an update plan that if executed will update the client to a version that is either equal to or as close to but
     * less than the desired version as possible. This plan normally contains one snapshot transition and zero or more delta
     * transitions but if no previous versions were found then an empty plan, {@code HollowUpdatePlan.DO_NOTHING}, is returned.
     * @param desiredVersionInfo Version info for the desired version to which the client wishes to update to, or update to as close to as possible but lesser than this version
     * @return An update plan containing 1 snapshot transition and 0+ delta transitions if requested versions were found,
     *         or an empty plan, {@code HollowUpdatePlan.DO_NOTHING}, if no previous versions were found
     */
    private HollowUpdatePlan snapshotPlan(HollowConsumer.VersionInfo desiredVersionInfo,
                                          HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) throws Exception {
        HollowUpdatePlan plan = new HollowUpdatePlan();
        long desiredVersion = desiredVersionInfo.getVersion();
        long nearestPreviousSnapshotVersion = includeNearestSnapshot(plan, desiredVersionInfo, updatePlanBlobVerifier);

        // The includeNearestSnapshot function returns a  snapshot version that is less than or equal to the desired version
        if(nearestPreviousSnapshotVersion > desiredVersion)
            return HollowUpdatePlan.DO_NOTHING;

        // If the nearest snapshot version is {@code HollowConstants.VERSION_LATEST} then no past snapshots were found, so
        // skip the delta planning and the update plan does nothing
        if(nearestPreviousSnapshotVersion == HollowConstants.VERSION_LATEST)
            return HollowUpdatePlan.DO_NOTHING;

        plan.appendPlan(this.deltaUpdatePlanner.deltaPlan(nearestPreviousSnapshotVersion, desiredVersion, Integer.MAX_VALUE));

        return plan;
    }

    private long includeNearestSnapshot(HollowUpdatePlan plan,
                                        HollowConsumer.VersionInfo desiredVersionInfo,
                                        HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) {
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
                    lookback++;
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
