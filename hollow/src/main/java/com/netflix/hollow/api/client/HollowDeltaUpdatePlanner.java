package com.netflix.hollow.api.client;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.core.HollowConstants;

public class HollowDeltaUpdatePlanner {

    private final HollowConsumer.BlobRetriever blobRetriever;

    public HollowDeltaUpdatePlanner(HollowConsumer.BlobRetriever blobRetriever) {
        this.blobRetriever = blobRetriever;
    }

    public HollowUpdatePlan deltaPlan(long currentVersion,
                                       long desiredVersion,
                                       int maxDeltas) {
        HollowUpdatePlan plan = new HollowUpdatePlan();
        if(currentVersion < desiredVersion) {
            applyForwardDeltasToPlan(currentVersion, desiredVersion, plan, maxDeltas);
        } else if(currentVersion > desiredVersion) {
            applyReverseDeltasToPlan(currentVersion, desiredVersion, plan, maxDeltas);
        }

        return plan;
    }

    private long applyForwardDeltasToPlan(long currentVersion,
                                          long desiredVersion,
                                          HollowUpdatePlan plan,
                                          int maxDeltas) {
        int transitionCounter = 0;

        while(currentVersion < desiredVersion && transitionCounter < maxDeltas) {
            currentVersion = includeNextDelta(plan, currentVersion, desiredVersion);
            transitionCounter++;
        }
        return currentVersion;
    }

    private long applyReverseDeltasToPlan(long currentVersion,
                                          long desiredVersion,
                                          HollowUpdatePlan plan,
                                          int maxDeltas) {
        long achievedVersion = currentVersion;
        int transitionCounter = 0;

        while (currentVersion > desiredVersion && transitionCounter < maxDeltas) {
            currentVersion = includeNextReverseDelta(plan, currentVersion);
            if (currentVersion != HollowConstants.VERSION_NONE)
                achievedVersion = currentVersion;
            transitionCounter++;
        }

        return achievedVersion;
    }

    /**
     * Includes the next delta only if it will not take us *after* the desired version
     */
    private long includeNextDelta(HollowUpdatePlan plan,
                                  long currentVersion,
                                  long desiredVersion) {
        HollowConsumer.Blob transition = blobRetriever.retrieveDeltaBlob(currentVersion);

        if(transition != null) {
            if(transition.getToVersion() <= desiredVersion) {
                plan.add(transition);
            }

            return transition.getToVersion();
        }

        return HollowConstants.VERSION_LATEST;
    }

    private long includeNextReverseDelta(HollowUpdatePlan plan,
                                         long currentVersion) {
        HollowConsumer.Blob transition = blobRetriever.retrieveReverseDeltaBlob(currentVersion);
        if(transition != null) {
            plan.add(transition);
            return transition.getToVersion();
        }

        return HollowConstants.VERSION_NONE;
    }
}
