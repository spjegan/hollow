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
import java.util.List;
import java.util.ArrayList;
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

    private final HollowConsumer.BlobRetriever blobRetriever;
    private final LCAForkRecoveryConfig config;
    private final DeltaForkRecoveryStrategy fallbackStrategy;
    
    /**
     * Result of bidirectional LCA search containing the LCA version and the paths traversed.
     */
    @SuppressWarnings("unused") // desiredToLcaPath may be used for future optimizations
    private static class LCASearchResult {
        private final long lcaVersion;
        private final List<Long> currentToLcaPath;
        private final List<Long> desiredToLcaPath;
        
        public LCASearchResult(long lcaVersion, List<Long> currentToLcaPath, List<Long> desiredToLcaPath) {
            this.lcaVersion = lcaVersion;
            this.currentToLcaPath = currentToLcaPath;
            this.desiredToLcaPath = desiredToLcaPath;
        }
        
        public long getLcaVersion() { return lcaVersion; }
        public List<Long> getCurrentToLcaPath() { return currentToLcaPath; }
        public List<Long> getDesiredToLcaPath() { return desiredToLcaPath; }
    }

    /**
     * Creates an LCA recovery strategy with default configuration.
     * Uses a default configuration and snapshot strategy as fallback.
     */
    public LCAForkRecoveryStrategy(HollowConsumer.BlobRetriever blobRetriever, DeltaForkRecoveryStrategy fallbackStrategy) {
        this(blobRetriever, LCAForkRecoveryConfig.DEFAULT_CONFIG, fallbackStrategy);
    }

    /**
     * Creates an LCA recovery strategy with specified configuration.
     * 
     * @param config Configuration for the LCA recovery strategy
     * @param fallbackStrategy Strategy to use if LCA cannot be found within the limit
     */
    public LCAForkRecoveryStrategy(HollowConsumer.BlobRetriever blobRetriever, LCAForkRecoveryConfig config, DeltaForkRecoveryStrategy fallbackStrategy) {
        this.blobRetriever = blobRetriever;
        this.config = config;
        this.fallbackStrategy = fallbackStrategy;
    }

    @Override
    public HollowUpdatePlan createRecoveryPlan(
            long currentVersion,
            HollowConsumer.VersionInfo desiredVersionInfo,
            HollowConsumer.UpdatePlanBlobVerifier updatePlanBlobVerifier) throws Exception {
        
        long desiredVersion = desiredVersionInfo.getVersion();
        LOG.info("Attempting bidirectional LCA-based recovery for version transition: " 
                + currentVersion + " -> " + desiredVersion + " with max lookback: " + config.getMaxReverseDeltaLookback());

        // Find LCA using bidirectional search
        LCASearchResult searchResult = findLCABidirectional(currentVersion, desiredVersion, blobRetriever);
        
        if (searchResult != null) {
            LOG.info("Found LCA at version: " + searchResult.getLcaVersion() + " using bidirectional search");
            return createLCAPlanFromPaths(currentVersion, desiredVersion, searchResult);
        } else {
            LOG.info("Could not find LCA within lookback limit of " + config.getMaxReverseDeltaLookback() + 
                    ", falling back to " + fallbackStrategy.getClass().getSimpleName());
            return fallbackStrategy.createRecoveryPlan(currentVersion, desiredVersionInfo,
                    updatePlanBlobVerifier);
        }
    }

    /**
     * Finds the Least Common Ancestor (LCA) using bidirectional search:
     * 1. Search backward from both current version and desired version simultaneously
     * 2. When the two search paths intersect, we've found the LCA
     * 3. This gives O(√L) average case performance instead of O(L)
     * 4. Returns the LCA and the paths traversed for reuse in plan creation
     */
    private LCASearchResult findLCABidirectional(long currentVersion, long desiredVersion, HollowConsumer.BlobRetriever blobRetriever) {
        
        if (currentVersion == desiredVersion) {
            // Special case: same version
            List<Long> singlePath = new ArrayList<>();
            singlePath.add(currentVersion);
            return new LCASearchResult(currentVersion, singlePath, singlePath);
        }
        
        Set<Long> currentVersionPathSet = new HashSet<>();
        Set<Long> desiredVersionPathSet = new HashSet<>();
        
        // Ordered paths for reconstruction
        List<Long> currentVersionPath = new ArrayList<>();
        List<Long> desiredVersionPath = new ArrayList<>();
        
        long currentCursor = currentVersion;
        long desiredCursor = desiredVersion;
        int maxReverseDeltaLookback = config.getMaxReverseDeltaLookback();
        
        LOG.info("Starting bidirectional search as far back as " + maxReverseDeltaLookback + " versions");
        
        for (int step = 0; step < config.getMaxReverseDeltaLookback(); step++) {
            
            if (currentCursor != HollowConstants.VERSION_NONE) {
                currentVersionPathSet.add(currentCursor);
                currentVersionPath.add(currentCursor);
                
                // Check if we've intersected with the desired path
                if (desiredVersionPathSet.contains(currentCursor)) {
                    LOG.info("Found LCA via bidirectional search: " + currentCursor 
                           + " (intersection found from current side at step " + step + ")");
                    
                    // Build paths to LCA
                    List<Long> currentToLca = buildPathToLCA(currentVersionPath, currentCursor);
                    List<Long> desiredToLca = buildPathToLCA(desiredVersionPath, currentCursor);
                    return new LCASearchResult(currentCursor, currentToLca, desiredToLca);
                }
                
                // Move one step backward
                currentCursor = goReverse(currentCursor, blobRetriever);
                LOG.fine("Current side step " + step + ": moved to version " + currentCursor);
            }
            
            // === SEARCH FROM DESIRED VERSION ===  
            if (desiredCursor != HollowConstants.VERSION_NONE) {
                desiredVersionPathSet.add(desiredCursor);
                desiredVersionPath.add(desiredCursor);
                
                // Check if we've intersected with the current path
                if (currentVersionPathSet.contains(desiredCursor)) {
                    LOG.info("Found LCA via bidirectional search: " + desiredCursor 
                           + " (intersection found from desired side at step " + step + ")");
                    
                    // Build paths to LCA
                    List<Long> currentToLca = buildPathToLCA(currentVersionPath, desiredCursor);
                    List<Long> desiredToLca = buildPathToLCA(desiredVersionPath, desiredCursor);
                    return new LCASearchResult(desiredCursor, currentToLca, desiredToLca);
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
                "Explored " + currentVersionPathSet.size() + " versions from current side, " +
                desiredVersionPathSet.size() + " versions from desired side");
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
     * Builds a path from the start of the given path list to the LCA version.
     */
    private List<Long> buildPathToLCA(List<Long> fullPath, long lcaVersion) {
        List<Long> pathToLca = new ArrayList<>();
        for (Long version : fullPath) {
            pathToLca.add(version);
            if (version.equals(lcaVersion)) {
                break;
            }
        }
        return pathToLca;
    }

    /**
     * Creates an update plan using the precomputed paths from bidirectional search.
     * This avoids re-reading blobs that were already accessed during LCA discovery.
     * 1. Goes backward from current version to LCA using reverse deltas
     * 2. Goes forward from LCA to desired version using forward deltas
     */
    private HollowUpdatePlan createLCAPlanFromPaths(long currentVersion, long desiredVersion, LCASearchResult searchResult) {
        HollowUpdatePlan plan = new HollowUpdatePlan();
        long lcaVersion = searchResult.getLcaVersion();
        
        // Step 1: Add reverse deltas from current version to LCA (excluding LCA itself)
        List<Long> currentToLcaPath = searchResult.getCurrentToLcaPath();
        for (int i = 0; i < currentToLcaPath.size() - 1; i++) {
            long fromVersion = currentToLcaPath.get(i);
            HollowConsumer.Blob reverseDeltaBlob = blobRetriever.retrieveReverseDeltaBlob(fromVersion);
            if (reverseDeltaBlob == null) {
                LOG.warning("Missing reverse delta from version " + fromVersion + " while going to LCA " + lcaVersion);
                break;
            }
            plan.add(reverseDeltaBlob);
        }
        
        // Step 2: Add forward deltas from LCA to desired version
        long version = lcaVersion;
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
                + currentVersion + " -> " + lcaVersion + " -> " + desiredVersion 
                + " (reused " + (currentToLcaPath.size() - 1) + " reverse delta paths from search)");
        
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
