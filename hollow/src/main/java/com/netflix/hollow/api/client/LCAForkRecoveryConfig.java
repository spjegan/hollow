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

/**
 * Configuration class for the LCA (Least Common Ancestor) fork recovery strategy.
 * Provides configurable parameters for controlling the behavior of the bidirectional LCA algorithm.
 */
public class LCAForkRecoveryConfig {
    
    /**
     * Default configuration with reasonable defaults.
     */
    public static final LCAForkRecoveryConfig DEFAULT_CONFIG = new LCAForkRecoveryConfig(100);
    
    private final int maxReverseDeltaLookback;
    
    /**
     * Creates a new configuration with the specified maximum reverse delta lookback.
     * 
     * @param maxReverseDeltaLookback Maximum number of versions to look back using reverse deltas.
     *                               Must be positive. This represents the total search budget for
     *                               the bidirectional algorithm (split between both directions).
     * @throws IllegalArgumentException if maxReverseDeltaLookback is not positive
     */
    public LCAForkRecoveryConfig(int maxReverseDeltaLookback) {
        if (maxReverseDeltaLookback <= 0) {
            throw new IllegalArgumentException("maxReverseDeltaLookback must be positive, got: " + maxReverseDeltaLookback);
        }
        this.maxReverseDeltaLookback = maxReverseDeltaLookback;
    }
    
    /**
     * Returns the maximum number of versions to look back using reverse deltas
     * when searching for the Least Common Ancestor. This is the total budget
     * for the bidirectional search (split between both directions).
     * 
     * @return the maximum lookback limit
     */
    public int getMaxReverseDeltaLookback() {
        return maxReverseDeltaLookback;
    }
    
    /**
     * Returns the search budget per direction for bidirectional search.
     * This is half of the total lookback limit.
     * 
     * @return the budget per direction
     */
    public int getBudgetPerDirection() {
        return Math.max(1, maxReverseDeltaLookback / 2);
    }
    
    /**
     * Creates a new configuration builder for customizing the LCA recovery behavior.
     * 
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder class for creating LCAForkRecoveryConfig instances.
     */
    public static class Builder {
        private int maxReverseDeltaLookback = 100;
        
        /**
         * Sets the maximum number of versions to look back using reverse deltas.
         * 
         * @param maxReverseDeltaLookback the maximum lookback limit (must be positive)
         * @return this builder instance
         * @throws IllegalArgumentException if maxReverseDeltaLookback is not positive
         */
        public Builder withMaxReverseDeltaLookback(int maxReverseDeltaLookback) {
            if (maxReverseDeltaLookback <= 0) {
                throw new IllegalArgumentException("maxReverseDeltaLookback must be positive, got: " + maxReverseDeltaLookback);
            }
            this.maxReverseDeltaLookback = maxReverseDeltaLookback;
            return this;
        }
        
        /**
         * Builds the configuration.
         * 
         * @return a new LCAForkRecoveryConfig instance
         */
        public LCAForkRecoveryConfig build() {
            return new LCAForkRecoveryConfig(maxReverseDeltaLookback);
        }
    }
    
    @Override
    public String toString() {
        return "LCAForkRecoveryConfig{" +
                "maxReverseDeltaLookback=" + maxReverseDeltaLookback +
                ", budgetPerDirection=" + getBudgetPerDirection() +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        LCAForkRecoveryConfig that = (LCAForkRecoveryConfig) o;
        return maxReverseDeltaLookback == that.maxReverseDeltaLookback;
    }
    
    @Override
    public int hashCode() {
        return maxReverseDeltaLookback;
    }
}
