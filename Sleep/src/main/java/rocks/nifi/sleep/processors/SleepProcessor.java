package rocks.nifi.sleep.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;


@SideEffectFree
@Tags({"SLEEP", "NIFI ROCKS"})
@CapabilityDescription("Sleep a flowfile for specific time.")
public class SleepProcessor extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final String MATCH_ATTR = "match";

    /*public static final PropertyDescriptor Sleep_time = new PropertyDescriptor.Builder()
            .name("Sleep time")
            .defaultValue("5")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    */

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    public static final Relationship SLEEP = new Relationship.Builder()
            .name("SLEEP")
            .description("Sleep relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();


    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
//        properties.add(Sleep_time);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(SLEEP);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();
        try {
            if (flowfile != null) {
                String sleepState = flowfile.getAttribute("_sleep_state");

                if (sleepState == null) {
                    flowfile = session.putAttribute(flowfile, "_sleep_state", "1");
                    flowfile = session.penalize(flowfile);
                    session.transfer(flowfile, SLEEP);
                } else {
                    flowfile = session.removeAttribute(flowfile, "_sleep_state");
                    session.transfer(flowfile, SUCCESS);
                }
            }
        } catch (Exception e) {
            session.transfer(flowfile, FAILURE);
        }

    }

}
