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
package rocks.nifi.writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.logging.ComponentLog;

@Tags({ "properties", "result", "set", "recordset", "record", "writer", "serializer", "row"})
@CapabilityDescription("Writes the contents of a RecordSet as Property data.")
public class PropertiesRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    public static final PropertyDescriptor FirstFieldName = new PropertyDescriptor.Builder()
            .name("first-field-name")
            .displayName("First FieldName")
            .description("This is the fieldname which is use to determine the key value of the Property.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("FirstField")
            .build();

    public static final PropertyDescriptor SecondFieldName = new PropertyDescriptor.Builder()
            .name("second-field-name")
            .displayName("Second FieldName")
            .description("This is the fieldname which is use to determine the value of the Property.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("SecondField")
            .build();


    String firstFieldName;
    String secondFieldName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>(super.getSupportedPropertyDescriptors());
	props.add(FirstFieldName);
	props.add(SecondFieldName);        
	return props;
    }


    @OnEnabled
    public void storePropertiesFormat(final ConfigurationContext context) {
        firstFieldName = context.getProperty(FirstFieldName).getValue();
        secondFieldName = context.getProperty(SecondFieldName).getValue();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) throws SchemaNotFoundException {
        return new WritePropertiesResult(schema, getSchemaAccessWriter(schema), out,
                getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null), firstFieldName, secondFieldName);
    }

}
