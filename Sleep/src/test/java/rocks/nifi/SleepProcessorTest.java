package rocks.nifi.sleep.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import static rocks.nifi.sleep.processors.SleepProcessor.SLEEP;
import static rocks.nifi.sleep.processors.SleepProcessor.SUCCESS;

public class SleepProcessorTest {
    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(SleepProcessor.class);
    }

    @Test
    public void testProcessor() throws IOException {
        //assertPenalizeCount
	List<MockFlowFile> originals = new ArrayList();        
	originals.add(testRunner.enqueue("sleep0"));
        originals.add(testRunner.enqueue("sleep1"));
        originals.add(testRunner.enqueue("sleep2"));

        testRunner.run(5);
        testRunner.assertAllFlowFilesTransferred(SLEEP, 3);
        testRunner.assertPenalizeCount(3);
        List<MockFlowFile> sleepedFlowFile = testRunner.getFlowFilesForRelationship(SLEEP);

        for(MockFlowFile sleeped : sleepedFlowFile) {
            sleeped.assertAttributeExists("_sleep_state");
            testRunner.enqueue(testRunner.getContentAsByteArray(sleeped), sleeped.getAttributes());
        }
	
	testRunner.clearTransferState();
        testRunner.run(5);

        testRunner.assertAllFlowFilesTransferred(SUCCESS, 3);
        testRunner.assertPenalizeCount(3);

        List<MockFlowFile> successFlowFile = testRunner.getFlowFilesForRelationship(SUCCESS);

        for(int i=0; i<successFlowFile.size();++i) {
            successFlowFile.get(i).assertAttributeNotExists("_sleep_state");
            successFlowFile.get(i).assertContentEquals(originals.get(i).toByteArray());
        }
    }
}
