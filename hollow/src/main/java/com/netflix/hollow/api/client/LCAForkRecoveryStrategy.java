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
import java.util.Set;
import java.util.HashSet;
import java.util.logging.Logger;

/**
 * Recovery strategy that uses bidirectional Least Common Ancestor (LCA) algorithm to find an optimal path
 * to the desired version when the direct delta path fails.
 * 
 * This strategy uses bidirectional search:
 * 1. Searches backward from both current version AND desired version simultaneously
 * 2. Finds the LCA when the two search paths intersect
 * 3. Creates a plan that goes backward to LCA, then forward to desired version
 * 4. Falls back to snapshot strategy if LCA cannot be found within the configured limit
 * 
 * Benefits of bidirectional search:
 * - Average case: O(√L) instead of O(L) where L is the lookback limit
 * - Faster convergence for recent forks (most common case)
 * - Same worst-case bounds with better practical performance
 */
public class LCAForkRecoveryStrategy implements DeltaForkRecoveryStrategy {
    private static final Logger LOG = Logger.getLogger(LCAForkRecoveryStrategy.class.getName());
    
    private final LCAForkRecoveryConfig config;
    private final DeltaForkRecoveryStrategy fallbackStrategy;

    /**
     * Creates an LCA recovery strategy with default configuration.
     * Uses a default configuration and snapshot strategy as fallback.
     */
    public LCAForkRecoveryStrategy() {
        this(LCAForkRecoveryConfig.DEFAULT_CONFIG, new SnapshotForkRecoveryStrategy());
    }

    /**
     * Creates an LCA recovery strategy with specified configuration.
     * 
     * @param config Configuration for the LCA recovery strategy
     * @param fallbackStrategy Strategy to use if LCA cannot be found within the limit
     */
    public LCAForkRecoveryStrategy(LCAForkRecoveryConfig config, DeltaForkRecoveryStrategy fallbackStrategy) {
        this.config = config;
        this.fallbackStrategy = fallbackStrategy;
    }

    /**
     * Creates an LCA recovery strategy with specified lookback limit.
     * 
     * @param maxReverseDeltaLookback Maximum number of versions to look back using reverse deltas
     * @param fallbackStrategy Strategy to use if LCA cannot be found within the limit
     */
    public LCAForkRecoveryStrategy(int maxReverseDeltaLookback, DeltaForkRecoveryStrategy fallbackStrategy) {
        this(new LCAForkRecoveryConfig(maxReverseDeltaLookback), fallbackStrategy);
    }

    @Override
    public HollowUpdatePlan createRecoveryPlan(
            long currentVersion,
            HollowConsumer.VersionInfo desiredVersionInfo,
            HollowConsumer.BlobRetriever blobRetriever,
            HollowConsumer.DoubleSnapshotConfig doubleSnapshotConfig,
            HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) throws Exception {
        
        long desiredVersion = desiredVersionInfo.getVersion();
        LOG.info("Attempting bidirectional LCA-based recovery for version transition: " 
                + currentVersion + " -> " + desiredVersion + " with max lookback: " + config.getMaxReverseDeltaLookback());

        // Find LCA using bidirectional search
        Long lcaVersion = findLCABidirectional(currentVersion, desiredVersion, blobRetriever);
        
        if (lcaVersion != null) {
            LOG.info("Found LCA at version: " + lcaVersion + " using bidirectional search");
            return createLCAPlan(currentVersion, lcaVersion, desiredVersion, blobRetriever);
        } else {
            LOG.info("Could not find LCA within lookback limit of " + config.getMaxReverseDeltaLookback() + 
                    ", falling back to " + fallbackStrategy.getClass().getSimpleName());
            return fallbackStrategy.createRecoveryPlan(currentVersion, desiredVersionInfo, blobRetriever, 
                    doubleSnapshotConfig, updatePlanBlobVerifier);
        }
    }

    /**
     * Finds the Least Common Ancestor (LCA) using bidirectional search:
     * 1. Search backward from both current version and desired version simultaneously
     * 2. When the two search paths intersect, we've found the LCA
     * 3. This gives O(√L) average case performance instead of O(L)
     */
    private Long findLCABidirectional(long currentVersion, long desiredVersion, HollowConsumer.BlobRetriever blobRetriever) {
        
        if (currentVersion == desiredVersion) {
            return currentVersion;
        }
        
        Set<Long> currentVersionPath = new HashSet<>();
        Set<Long> desiredVersionPath = new HashSet<>();
        
        long currentCursor = currentVersion;
        long desiredCursor = desiredVersion;
        int budgetPerDirection = config.getBudgetPerDirection();
        
        LOG.fine("Starting bidirectional search with budget " + budgetPerDirection + " per direction");
        
        for (int step = 0; step < budgetPerDirection; step++) {
            
            // === SEARCH FROM CURRENT VERSION ===
            if (currentCursor != HollowConstants.VERSION_NONE) {
                currentVersionPath.add(currentCursor);
                
                // Check if we've intersected with the desired path
                if (desiredVersionPath.contains(currentCursor)) {
                    LOG.info("Found LCA via bidirectional search: " + currentCursor 
                           + " (intersection found from current side at step " + step + ")");
                    return currentCursor;
                }
                
                // Move one step backward
                currentCursor = goReverse(currentCursor, blobRetriever);
                LOG.fine("Current side step " + step + ": moved to version " + currentCursor);
            }
            
            // === SEARCH FROM DESIRED VERSION ===  
            if (desiredCursor != HollowConstants.VERSION_NONE) {
                desiredVersionPath.add(desiredCursor);
                
                // Check if we've intersected with the current path
                if (currentVersionPath.contains(desiredCursor)) {
                    LOG.info("Found LCA via bidirectional search: " + desiredCursor 
                           + " (intersection found from desired side at step " + step + ")");
                    return desiredCursor;
                }
                
                // Move one step backward
                desiredCursor = goReverse(desiredCursor, blobRetriever);
                LOG.fine("Desired side step " + step + ": moved to version " + desiredCursor);
            }
            
            // Early termination if both cursors are exhausted
            if (currentCursor == HollowConstants.VERSION_NONE && desiredCursor == HollowConstants.VERSION_NONE) {
                LOG.fine("Both search directions exhausted at step " + step);
                break;
            }
        }
        
        LOG.info("No LCA found via bidirectional search within budget. " +
                "Explored " + currentVersionPath.size() + " versions from current side, " +
                desiredVersionPath.size() + " versions from desired side");
        return null;
    }

    /**
     * Helper method to move one step backward using reverse delta.
     */
    private long goReverse(long version, HollowConsumer.BlobRetriever blobRetriever) {
        HollowConsumer.Blob reverseDelta = blobRetriever.retrieveReverseDeltaBlob(version);
        return reverseDelta != null ? reverseDelta.getToVersion() : HollowConstants.VERSION_NONE;
    }

    /**
     * Creates an update plan that:
     * 1. Goes backward from current version to LCA using reverse deltas
     * 2. Goes forward from LCA to desired version using forward deltas
     */
    private HollowUpdatePlan createLCAPlan(long currentVersion, long lcaVersion, long desiredVersion, HollowConsumer.BlobRetriever blobRetriever) {
        HollowUpdatePlan plan = new HollowUpdatePlan();
        
        // Step 1: Go backward from current version to LCA
        long version = currentVersion;
        while (version > lcaVersion) {
            HollowConsumer.Blob reverseDeltaBlob = blobRetriever.retrieveReverseDeltaBlob(version);
            if (reverseDeltaBlob == null) {
                LOG.warning("Missing reverse delta from version " + version + " while going to LCA " + lcaVersion);
                break;
            }
            plan.add(reverseDeltaBlob);
            version = reverseDeltaBlob.getToVersion();
        }
        
        // Step 2: Go forward from LCA to desired version
        version = lcaVersion;
        while (version < desiredVersion) {
            HollowConsumer.Blob deltaBlob = blobRetriever.retrieveDeltaBlob(version);
            if (deltaBlob == null) {
                LOG.warning("Missing delta from version " + version + " while going to desired version " + desiredVersion);
                break;
            }
            
            if (deltaBlob.getToVersion() <= desiredVersion) {
                plan.add(deltaBlob);
            }
            
            version = deltaBlob.getToVersion();
        }
        
        LOG.info("Created bidirectional LCA plan with " + plan.numTransitions() + " transitions: " 
                + currentVersion + " -> " + lcaVersion + " -> " + desiredVersion);
        
        return plan;
    }

    /**
     * Returns the configuration used by this LCA recovery strategy.
     */
    public LCAForkRecoveryConfig getConfig() {
        return config;
    }

    /**
     * Returns the maximum number of reverse delta versions to look back.
     */
    public int getMaxReverseDeltaLookback() {
        return config.getMaxReverseDeltaLookback();
    }

    /**
     * Returns the fallback strategy used when LCA cannot be found.
     */
    public DeltaForkRecoveryStrategy getFallbackStrategy() {
        return fallbackStrategy;
    }
}
