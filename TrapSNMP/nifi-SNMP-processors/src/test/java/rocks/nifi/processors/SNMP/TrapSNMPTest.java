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
package rocks.nifi.processors.SNMP;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.HashMap;

import static rocks.nifi.processors.SNMP.TrapSNMP.REL_SUCCESS;
import static rocks.nifi.processors.SNMP.TrapSNMP.REL_FAILURE;


public class TrapSNMPTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(TrapSNMP.class);
    }

    @Test
    public void testProcessor() {
		Map<String, String> attributesGood = new HashMap();
		attributesGood.put("snmp$1.3.6.1.2.1.1.4.0", "Vinicius");
		testRunner.enqueue("", attributesGood);
		
		testRunner.run();
		
		testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
		
		testRunner.clearTransferState();
		
		attributesGood.put("snmp$1.3.6.1.2.1.1.4.0$2", "123");
		testRunner.enqueue("", attributesGood);

		testRunner.run();
		
		testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
		
		testRunner.clearTransferState();
		
		Map<String, String> attributesFailFirst = new HashMap();
		attributesFailFirst.put("snmp$1.3.6.1.2.1.1.4.0$2", "Vinicius");
		testRunner.enqueue("", attributesFailFirst);
		
		testRunner.run();
		
		testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
		
		testRunner.clearTransferState();
		
		Map<String, String> attributesFailSecond = new HashMap();
		attributesFailSecond.put("snmp$string", "Vinicius");
		testRunner.enqueue("", attributesFailSecond);
		
		testRunner.run();
		
		testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

}
