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
package com.netflix.hollow.api.producer.validation;

import com.netflix.hollow.api.producer.HollowProducer.ReadState;
import com.netflix.hollow.core.read.engine.HollowTypeReadState;
import java.util.function.Supplier;

/**
 * Used to validate if the cardinality change in current cycle is with in the allowed percent for a given typeName.
 * Ex: 0% allowableVariancePercent ensures type cardinality does not vary at all for cycle to cycle.
 * Ex: Number of state in United States.
 * 10% allowableVariancePercent: from previous cycle any addition or removal within 10% cardinality is valid.
 * Anything more results in failure of validation.
 *
 * @author lkanchanapalli {@literal<lavanya65@yahoo.com>}
 */
public class RecordCountVarianceValidator implements ValidatorListener {
    private static final String ZERO_PREVIOUS_COUNT_WARN_MSG_FORMAT =
            "Previous record count is 0. Not running RecordCountVarianceValidator for type %s. "
                    + "This scenario is not expected except when starting a new namespace.";
    private static final String FAILED_RECORD_COUNT_VALIDATION =
            "Record count validation for type %s has failed as actual change percent %s "
                    + "is greater than allowed change percent %s.";

    private static final String NULL_THRESHOLD =
            "Record count validation for type %s has failed because the variance threshold was null.";

    private static final String DATA_TYPE_NAME = "Typename";
    private static final String ALLOWABLE_VARIANCE_PERCENT_NAME = "AllowableVariancePercent";
    private static final String LATEST_CARDINALITY_NAME = "LatestRecordCount";
    private static final String PREVIOUS_CARDINALITY_NAME = "PreviousRecordCount";
    private static final String ACTUAL_CHANGE_PERCENT_NAME = "ActualChangePercent";

    private static final String NAME = RecordCountVarianceValidator.class.getName();

    private final String typeName;

    private final Supplier<Float> allowableVariancePercentSupplier;

    /**
     * @param typeName type name
     * @param allowableVariancePercent: Used to validate if the cardinality change in current cycle is with in the
     * allowed percent.
     * Ex: 0% allowableVariancePercent ensures type cardinality does not vary at all for cycle to cycle.
     * Ex: Number of state in United States.
     * 10% allowableVariancePercent: from previous cycle any addition or removal within 10% cardinality is valid.
     * Anything more results in failure of validation.
     */
    public RecordCountVarianceValidator(String typeName, float allowableVariancePercent) {
        this(typeName, () -> allowableVariancePercent);
    }

    /**
     * @param typeName type name
     * @param allowableVariancePercentSupplier: Used to validate if the cardinality change in current cycle is with in the
     * allowed percent, and changes to this threshold are applied in the next invocation of validation.
     * Ex: 0% allowableVariancePercent ensures type cardinality does not vary at all for cycle to cycle.
     * Ex: Number of state in United States.
     * 10% allowableVariancePercent: from previous cycle any addition or removal within 10% cardinality is valid.
     * Anything more results in failure of validation.
     */
    public RecordCountVarianceValidator(String typeName, Supplier<Float> allowableVariancePercentSupplier)  {
        this.typeName = typeName;
        this.allowableVariancePercentSupplier = allowableVariancePercentSupplier;
        Float allowableVariancePercent = allowableVariancePercentSupplier.get();
        if (allowableVariancePercent == null || allowableVariancePercent < 0) {
            throw new IllegalArgumentException("RecordCountVarianceValidator for type " + typeName
                    + ": cannot have allowableVariancePercent be null or less than 0. Value provided: "
                    + allowableVariancePercent);
        }
    }

    @Override
    public String getName() {
        return NAME + "_" + typeName;
    }

    @Override
    public ValidationResult onValidate(ReadState readState) {
        ValidationResult.ValidationResultBuilder vrb = ValidationResult.from(this);

        Float allowableVariancePercent = allowableVariancePercentSupplier.get();
        if (allowableVariancePercent == null) {
            String message = String.format(NULL_THRESHOLD, typeName);
            return vrb.failed(message);
        }

        vrb.detail(ALLOWABLE_VARIANCE_PERCENT_NAME, allowableVariancePercent)
                .detail(DATA_TYPE_NAME, typeName);

        HollowTypeReadState typeState = readState.getStateEngine().getTypeState(typeName);
        int latestCardinality = typeState.getPopulatedOrdinals().cardinality();
        int previousCardinality = typeState.getPreviousOrdinals().cardinality();
        vrb.detail(LATEST_CARDINALITY_NAME, latestCardinality)
                .detail(PREVIOUS_CARDINALITY_NAME, previousCardinality);

        if (previousCardinality == 0) {
            return vrb.detail("skipped", Boolean.TRUE).
                    passed(String.format(ZERO_PREVIOUS_COUNT_WARN_MSG_FORMAT, typeName));
        }

        float actualChangePercent = getChangePercent(latestCardinality, previousCardinality);
        vrb.detail(ACTUAL_CHANGE_PERCENT_NAME, actualChangePercent);

        if (Float.compare(actualChangePercent, allowableVariancePercent) > 0) {
            String message = String.format(FAILED_RECORD_COUNT_VALIDATION, typeName, actualChangePercent,
                    allowableVariancePercent);
            return vrb.failed(message);
        }

        return vrb.passed(String.format("%s percentChange=%.2f%%, threshold=%.2f%%",
                getName(), actualChangePercent, allowableVariancePercent));
    }

    // protected for tests
    float getChangePercent(int latestCardinality, int previousCardinality) {
        int diff = Math.abs(latestCardinality - previousCardinality);
        return (100.0f * diff) / previousCardinality;
    }
}
	
