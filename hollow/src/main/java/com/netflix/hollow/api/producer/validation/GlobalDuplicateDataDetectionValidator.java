/*
 *  Copyright 2024 - 2025 Netflix, Inc.
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

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.core.index.HollowPrimaryKeyIndex;
import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.engine.HollowTypeReadState;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowSchema;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class GlobalDuplicateDataDetectionValidator implements ValidatorListener {

    private static final Logger logger = Logger.getLogger(GlobalDuplicateDataDetectionValidator.class.getName());

    private boolean strictMode = false;

    public GlobalDuplicateDataDetectionValidator(final boolean strictMode) {
        this.strictMode = strictMode;
    }

    @Override
    public ValidationResult onValidate(HollowProducer.ReadState readState) {
        ValidationResult.ValidationResultBuilder vrb = ValidationResult.from(this);
        
        HollowReadStateEngine stateEngine = readState.getStateEngine();
        Collection<HollowTypeReadState> typeReadStates = stateEngine.getTypeStates();
        Stream<HollowTypeReadState> hollowTypeReadStateStream = typeReadStates
                .stream()
                .filter(typeReadState ->
                        isACandidateForDuplicateDetection(typeReadState) && hasDuplicates(stateEngine, typeReadState));
        long duplicateRecordCount = hollowTypeReadStateStream.count();
        if (duplicateRecordCount == 0) {
            vrb.passed();
        }

        if (this.strictMode) {
            vrb.failed(String.format("%d duplicate records detected", duplicateRecordCount));
        } else {
            // TODO: Add a metric here
            vrb.passed();
        }

//        List<HollowSchema> schemas = readState.getStateEngine().getSchemas();
//        ValidationResult.ValidationResultBuilder vrb = ValidationResult.from(this);
//        schemas
//                .stream()
//                .filter(schema -> schema.getSchemaType().equals(HollowSchema.SchemaType.OBJECT))
//                .map(schema -> (HollowObjectSchema) schema)
//                .map(objectSchema -> {
//
//                });

        return vrb.passed();
    }

    private boolean isACandidateForDuplicateDetection(HollowTypeReadState typeReadState) {
        HollowSchema hollowSchema = typeReadState.getSchema();
        if (!hollowSchema.getSchemaType().equals(HollowSchema.SchemaType.OBJECT)) {
            return false;
        }

        HollowObjectSchema objectSchema = (HollowObjectSchema) hollowSchema;
        if (objectSchema.getPrimaryKey() == null) {
            // TODO: Add a metric here to count these cases? And should the level be "WARN" instead?
            logger.log(Level.INFO, String.format("Schema %s does not have a PrimaryKey defined. Skipping global duplicate detection.", objectSchema.getName()));
            return false;
        }

        return true;
    }

    private boolean hasDuplicates(HollowReadStateEngine stateEngine, HollowTypeReadState typeReadState) {
        HollowPrimaryKeyIndex hollowPrimaryKeyIndex = typeReadState.getListener(HollowPrimaryKeyIndex.class);
        if (hollowPrimaryKeyIndex == null) {
            HollowObjectSchema objectSchema = (HollowObjectSchema) typeReadState.getSchema();
            PrimaryKey primaryKey = objectSchema.getPrimaryKey();
            hollowPrimaryKeyIndex = new HollowPrimaryKeyIndex(stateEngine, primaryKey);
        }
        return !hollowPrimaryKeyIndex.getDuplicateKeys().isEmpty();
    }

    @Override
    public String getName() {
        return GlobalDuplicateDataDetectionValidator.class.getName();
    }
}
