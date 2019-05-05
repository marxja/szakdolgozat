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
package rocks.nifi.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static rocks.nifi.processors.SplitYaml.REL_ORIGINAL;
import static rocks.nifi.processors.SplitYaml.REL_SPLIT;
import static rocks.nifi.processors.SplitYaml.REL_FAILURE;

public class SplitYamlTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(SplitYaml.class);
    }

    @Test
    public void testProcessor() throws IOException {
        MockFlowFile originalFirst = testRunner.enqueue("---\n" +
                "- Apple\n" +
                "- Banana\n" +
                "- Grapefruit");

        testRunner.run();

        testRunner.assertTransferCount(REL_SPLIT, 3);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);

        List<MockFlowFile> splittedFirst = testRunner.getFlowFilesForRelationship(REL_SPLIT);

        splittedFirst.get(0).assertContentEquals("Apple\n");
        splittedFirst.get(1).assertContentEquals("Banana\n");
        splittedFirst.get(2).assertContentEquals("Grapefruit\n");

        List<MockFlowFile> originalsFirst = testRunner.getFlowFilesForRelationship(REL_ORIGINAL);

        originalsFirst.get(0).assertContentEquals(originalFirst.toByteArray());
	
	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "fragment.identifier");
	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "fragment.index");
	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "fragment.count");
	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "segment.original.filename");

	splittedFirst.get(0).assertAttributeEquals("fragment.identifier", splittedFirst.get(1).getAttribute("fragment.identifier"));
	splittedFirst.get(1).assertAttributeEquals("fragment.identifier", splittedFirst.get(2).getAttribute("fragment.identifier"));

	splittedFirst.get(0).assertAttributeEquals("fragment.index", "1");
	splittedFirst.get(1).assertAttributeEquals("fragment.index", "2");
	splittedFirst.get(2).assertAttributeEquals("fragment.index", "3");

	splittedFirst.get(0).assertAttributeEquals("fragment.count", "3");
	splittedFirst.get(1).assertAttributeEquals("fragment.count", "3");
	splittedFirst.get(2).assertAttributeEquals("fragment.count", "3");
	
	splittedFirst.get(0).assertAttributeEquals("segment.original.filename", splittedFirst.get(1).getAttribute("segment.original.filename"));
	splittedFirst.get(1).assertAttributeEquals("segment.original.filename", splittedFirst.get(2).getAttribute("segment.original.filename"));

        testRunner.clearTransferState();

        MockFlowFile originalSecond = testRunner.enqueue("---\n" +
                "-  martin:\n" +
                "    name: Martin D'vloper\n" +
                "    job: Developer\n" +
                "    skills:\n" +
                "      - python\n" +
                "      - perl\n" +
                "      - pascal\n" +
                "-  tabitha:\n" +
                "    name: Tabitha Bitumen\n" +
                "    job: Developer\n" +
                "    skills:\n" +
                "      - lisp\n" +
                "      - fortran\n" +
                "      - erlang");

        testRunner.run();

        testRunner.assertTransferCount(REL_SPLIT, 2);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);

        List<MockFlowFile> splittedSecond = testRunner.getFlowFilesForRelationship(REL_SPLIT);

        splittedSecond.get(0).assertContentEquals("martin:\n" +
                "  name: Martin D'vloper\n" +
                "  job: Developer\n" +
                "  skills: [python, perl, pascal]\n");
        splittedSecond.get(1).assertContentEquals("tabitha:\n" +
                "  name: Tabitha Bitumen\n" +
                "  job: Developer\n" +
                "  skills: [lisp, fortran, erlang]\n");

        List<MockFlowFile> originalsSecond = testRunner.getFlowFilesForRelationship(REL_ORIGINAL);

        originalsSecond.get(0).assertContentEquals(originalSecond.toByteArray());

	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "fragment.identifier");
	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "fragment.index");
	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "fragment.count");
	testRunner.assertAllFlowFilesContainAttribute(REL_SPLIT, "segment.original.filename");

	splittedSecond.get(0).assertAttributeEquals("fragment.identifier", splittedSecond.get(1).getAttribute("fragment.identifier"));

	splittedSecond.get(0).assertAttributeEquals("fragment.index", "1");
	splittedSecond.get(1).assertAttributeEquals("fragment.index", "2");

	splittedSecond.get(0).assertAttributeEquals("fragment.count", "2");
	splittedSecond.get(1).assertAttributeEquals("fragment.count", "2");
	
	splittedSecond.get(0).assertAttributeEquals("segment.original.filename", splittedSecond.get(1).getAttribute("segment.original.filename"));

        testRunner.clearTransferState();


        MockFlowFile originalNotList = testRunner.enqueue("name: Kovacs Lajos\n" +
                "job: Developer\n" +
                "skills:\n" +
                "   - lisp\n" +
                "   - fortran\n" +
                "   - c++");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);

        List<MockFlowFile> originalsNotList = testRunner.getFlowFilesForRelationship(REL_FAILURE);

        originalsNotList.get(0).assertContentEquals(originalNotList.toByteArray());

        testRunner.clearTransferState();


        MockFlowFile originalNotYml = testRunner.enqueue("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\n" +
                "\"Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);

        List<MockFlowFile> originalsNotYml = testRunner.getFlowFilesForRelationship(REL_FAILURE);

        originalsNotYml.get(0).assertContentEquals(originalNotYml.toByteArray());

    }

}
