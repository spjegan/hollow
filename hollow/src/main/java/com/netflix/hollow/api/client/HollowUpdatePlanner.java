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
 * The HollowUpdatePlanner defines the logic responsible for interacting with a {@link HollowBlobRetriever} 
 * to create a {@link HollowUpdatePlan}.
 * 
 * <p>When a direct delta path cannot reach the desired version, the planner uses a recovery strategy:
 * <ul>
 *   <li><b>Default behavior:</b> Uses {@link SnapshotForkRecoveryStrategy} when allowSnapshot=true</li>
 *   <li><b>Advanced recovery:</b> Uses configured {@link DeltaForkRecoveryStrategy} when allowSnapshot=false</li>
 *   <li><b>Initialization:</b> Always uses {@link SnapshotForkRecoveryStrategy} for VERSION_NONE</li>
 * </ul>
 * 
 * <p>To use LCA-based recovery instead of the default snapshot approach:
 * <pre>{@code
 * // Enable LCA recovery with default config
 * HollowUpdatePlanner planner = HollowUpdatePlanner.withLCARecovery(blobRetriever);
 * 
 * // Custom LCA config
 * LCAForkRecoveryConfig config = LCAForkRecoveryConfig.builder()
 *     .withMaxReverseDeltaLookback(50)
 *     .build();
 * HollowUpdatePlanner planner = HollowUpdatePlanner.withLCARecovery(blobRetriever, config);
 * }</pre>
 */
public class HollowUpdatePlanner {
    private static final Logger LOG = Logger.getLogger(HollowUpdatePlanner.class.getName());

    private final HollowConsumer.BlobRetriever transitionCreator;
    private final HollowConsumer.DoubleSnapshotConfig doubleSnapshotConfig;
    private final HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier;
    private final HollowDeltaUpdatePlanner deltaUpdatePlanner;
    private final DeltaForkRecoveryStrategy snapshotForkRecoveryStrategy;

    @Deprecated
    public HollowUpdatePlanner(HollowBlobRetriever blobRetriever) {
        this(HollowClientConsumerBridge.consumerBlobRetrieverFor(blobRetriever));
    }

    public HollowUpdatePlanner(HollowConsumer.BlobRetriever blobRetriever) {
        this(blobRetriever, HollowConsumer.DoubleSnapshotConfig.DEFAULT_CONFIG);
    }

    public HollowUpdatePlanner(HollowConsumer.BlobRetriever transitionCreator, HollowConsumer.DoubleSnapshotConfig doubleSnapshotConfig) {
        this(transitionCreator, doubleSnapshotConfig, HollowConsumer.UpdatePlanBlobVerifier.DEFAULT_INSTANCE);
    }

    public HollowUpdatePlanner(HollowConsumer.BlobRetriever transitionCreator,
                               HollowConsumer.DoubleSnapshotConfig doubleSnapshotConfig,
                               HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) {
        this.transitionCreator = transitionCreator;
        this.doubleSnapshotConfig = doubleSnapshotConfig;
        this.updatePlanBlobVerifier = updatePlanBlobVerifier;
        this.deltaUpdatePlanner = new HollowDeltaUpdatePlanner(transitionCreator);
        this.snapshotForkRecoveryStrategy = new SnapshotForkRecoveryStrategy(transitionCreator);
    }

    /**
     * @deprecated use {@link #planInitializingUpdate(HollowConsumer.VersionInfo)} instead.
     */
    @Deprecated
    public HollowUpdatePlan planInitializingUpdate(long desiredVersion) throws Exception {
        return planInitializingUpdate(new HollowConsumer.VersionInfo(desiredVersion));
    }

    /**
     * @return the sequence of steps necessary to initialize a hollow state engine to a given state.
     * @param desiredVersionInfo - The version to which the hollow state engine should be updated once the resultant steps are applied.
     * @throws Exception if the plan cannot be initialized
     */
    public HollowUpdatePlan planInitializingUpdate(HollowConsumer.VersionInfo desiredVersionInfo) throws Exception {
        return planUpdate(HollowConstants.VERSION_NONE, desiredVersionInfo, true);
    }

    /**
     * @deprecated use {@link #planUpdate(long, HollowConsumer.VersionInfo, boolean)} instead.
     */
    @Deprecated
    public HollowUpdatePlan planUpdate(long currentVersion, long desiredVersion, boolean allowSnapshot) throws Exception {
        return planUpdate(currentVersion, new HollowConsumer.VersionInfo(desiredVersion), allowSnapshot);
    }

    /**
     * Create an update plan from the current version to the desired version.
     * 
     * Recovery Strategy Selection:
     * - If allowSnapshot=true: Uses SnapshotForkRecoveryStrategy for delta fork recovery
     * - If allowSnapshot=false: Uses the configured deltaForkRecoveryStrategy (typically LCA-based)
     * - For initialization (currentVersion=VERSION_NONE): Always uses SnapshotForkRecoveryStrategy
     *
     * @param currentVersion - The current version of the hollow state engine, or HollowConstants.VERSION_NONE if not yet initialized
     * @param desiredVersionInfo - The version info to which the hollow state engine should be updated once the resultant steps are applied.
     * @param allowSnapshot - Controls recovery strategy selection when delta plan fails to reach desired version
     * @return the sequence of steps necessary to bring a hollow state engine up to date.
     * @throws Exception if the plan cannot be updated
     */
    public HollowUpdatePlan planUpdate(long currentVersion, HollowConsumer.VersionInfo desiredVersionInfo, boolean allowSnapshot) throws Exception {
        long desiredVersion = desiredVersionInfo.getVersion();

        if(desiredVersion == currentVersion)
            return HollowUpdatePlan.DO_NOTHING;

        if (currentVersion == HollowConstants.VERSION_NONE) {
            // For initialization, always use snapshot recovery strategy
            return snapshotForkRecoveryStrategy.createRecoveryPlan(currentVersion, desiredVersionInfo, updatePlanBlobVerifier);
        }

        HollowUpdatePlan deltaPlan = deltaUpdatePlanner.deltaPlan(currentVersion, desiredVersion, doubleSnapshotConfig.maxDeltasBeforeDoubleSnapshot());

        long deltaDestinationVersion = deltaPlan.destinationVersion(currentVersion);
        if(deltaDestinationVersion != desiredVersion) {
            DeltaForkRecoveryStrategy deltaForkRecoveryStrategy = resolveForkRecoveryStrategy(allowSnapshot);
            if (deltaForkRecoveryStrategy != null) {
                HollowUpdatePlan recoveryPlan = deltaForkRecoveryStrategy.createRecoveryPlan(
                        currentVersion, desiredVersionInfo, updatePlanBlobVerifier);

                if (recoveryPlan != null) {
                    long recoveryDestinationVersion = recoveryPlan.destinationVersion(currentVersion);

                    // Use recovery plan if it reaches the desired version or gets closer than the delta plan
                    if (recoveryDestinationVersion == desiredVersion
                            || ((deltaDestinationVersion > desiredVersion) && (recoveryDestinationVersion < desiredVersion))
                            || ((recoveryDestinationVersion < desiredVersion) && (recoveryDestinationVersion > deltaDestinationVersion))) {
                        return recoveryPlan;
                    }
                }
            }
        }

        return deltaPlan;
    }

    private DeltaForkRecoveryStrategy resolveForkRecoveryStrategy(boolean allowSnapshot) {
        // Prefer snapshot recovery strategy when allowed.
        if (allowSnapshot) {
            LOG.fine("Using SnapshotForkRecoveryStrategy for delta fork recovery (allowSnapshot=true)");
            return new SnapshotForkRecoveryStrategy(transitionCreator);
        }

        // TODO-Jegan: Check the feature flag before enabling LCA fork recovery strategy.
//        LOG.fine("Using LCAForkRecoveryStrategy for delta fork recovery (allowSnapshot=false)");
//        return new LCAForkRecoveryStrategy(transitionCreator, snapshotForkRecoveryStrategy);
        return null;
    }
}
