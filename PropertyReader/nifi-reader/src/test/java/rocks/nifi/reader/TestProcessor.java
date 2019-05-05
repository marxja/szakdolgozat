/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rocks.nifi.reader;

import java.util.ArrayList;
import java.util.List;
import java.lang.Exception;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import java.nio.charset.StandardCharsets;
import org.apache.nifi.serialization.record.RecordSchema;


import java.util.Map;
import java.io.InputStream;
import java.util.Set;

public class TestProcessor extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully read will be routed to this relationship")
            .build();
    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be read from the configured input format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

            final Map<String, String> originalAttributes = flowFile.getAttributes();

            final InputStream in = session.read(flowFile);

            final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, getLogger());

            final RecordSchema schema = reader.getSchema();

            List<String> fieldNames = schema.getFieldNames();

            Record elem = reader.nextRecord();
            while (elem != null) {
                FlowFile split = session.create(flowFile);
                for(String fieldName : fieldNames) {
                    split = session.putAttribute(split, fieldName, elem.getAsString(fieldName));
                }

                session.transfer(split, SUCCESS);
            }
        } catch(Exception e) {
            session.transfer(flowFile, FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(RECORD_READER);
        return propDescs;
    }
}
