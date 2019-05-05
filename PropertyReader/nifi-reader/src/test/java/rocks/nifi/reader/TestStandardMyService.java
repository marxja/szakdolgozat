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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;
import java.util.List;


import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.DateTimeUtils;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static rocks.nifi.reader.TestProcessor.SUCCESS;
import static rocks.nifi.reader.TestProcessor.FAILURE;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;


public class TestStandardMyService {

    TestRunner testRunner;
    //PropertiesReader service;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(TestProcessor.class);
        //service = new PropertiesReader();
    }

    @Test
    public void testService() throws InitializationException {
/*        testRunner.addControllerService("test", service);
        testRunner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        testRunner.setProperty(service, SchemaAccessUtils.SCHEMA_TEXT, "{\n" +
                "  \"name\": \"recordFormatName\",\n" +
                "  \"namespace\": \"nifi.examples\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\": \"key\", \"type\": \"string\" },\n" +
                "    { \"name\": \"value\", \"type\": \"int\" }\n" +
                "  ]\n" +
                "}");
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);

        // configure the processor and link it with the service
        testRunner.setProperty(TestProcessor.RECORD_READER, "test");
*/        testRunner.assertValid();

        testRunner.enqueue("index = 12\n" +
                "elem = 345");


        testRunner.run();
/*
        testRunner.assertAllFlowFilesTransferred(SUCCESS, 2);

        List<MockFlowFile> firstTest = testRunner.getFlowFilesForRelationship(SUCCESS);

        firstTest.get(0).assertAttributeEquals("key", "index");
        firstTest.get(0).assertAttributeEquals("value", "12");

        firstTest.get(1).assertAttributeEquals("key", "elem");
        firstTest.get(1).assertAttributeEquals("value", "345");

        /*
        testRunner.clearTransferState();

        testRunner.disableControllerService(service);

        testRunner.setProperty(service, SchemaAccessUtils.SCHEMA_TEXT, "{\n" +
                "  \"name\": \"recordFormatName\",\n" +
                "  \"namespace\": \"nifi.examples\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\": \"key\", \"type\": \"string\" },\n" +
                "    { \"name\": \"value\", \"type\": \"date\" }\n" +
                "  ]\n" +
                "}");
        testRunner.setProperty(service, DateTimeUtils.DATE_FORMAT, "MM/dd/yyyy");
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);

        testRunner.enqueue("begin = 01/01/2007\n" +
                "end = 03/13/2008");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SUCCESS, 2);

        List<MockFlowFile> secondTest = testRunner.getFlowFilesForRelationship(SUCCESS);

        secondTest.get(0).assertAttributeEquals("key", "begin");
        secondTest.get(0).assertAttributeEquals("value", "01/01/2007");

        secondTest.get(1).assertAttributeEquals("key", "end");
        secondTest.get(1).assertAttributeEquals("value", "03/13/2008");


        testRunner.clearTransferState();

        testRunner.enqueue("name = Kovacs\n" +
                "job = Developer");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FAILURE, 1);


        testRunner.clearTransferState();

        testRunner.enqueue("nem property tipusu file");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FAILURE, 1);
        */
    }

}
