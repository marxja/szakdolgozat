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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.exception.MissingFlowFileException;

import org.yaml.snakeyaml.Yaml;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"yaml", "split"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits a Yaml File into multiple, separate FlowFiles for an array element. "
        + "Each generated FlowFile is comprised of an element of the specified array and transferred to relationship 'split,' "
        + "with the original file transferred to the 'original' relationship. If the specified File "
        + "does not evaluate to an array element, the original file is routed to 'failure' and no files are generated.")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count",
                description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content (as an object) is read into memory, " +
        "in addition to all of the generated FlowFiles representing the split Yaml. If many splits are generated due to the size of the Yaml, or how the Yaml is " +
        "configured to be split, a two-phase approach may be necessary to avoid excessive use of memory.")
public class SplitYaml extends AbstractProcessor {


    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to "
                    + "this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid YAML)")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;




    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SPLIT);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(ProcessContext processContext) {
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) {
        FlowFile original = processSession.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        Yaml yaml = new Yaml();
        InputStream inputStream;
        try {
            inputStream = processSession.read(original);
        } catch (MissingFlowFileException e) {
            logger.error("the given FlowFile {} content cannot be found", new Object[]{original});
            processSession.transfer(original, REL_FAILURE);
            return;
        }


        Object yamlResult;
        yamlResult = yaml.load(inputStream);

        try {
            inputStream.close();
        } catch(IOException e) {
            logger.error("I/O error occurs");
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        if (!(yamlResult instanceof List)) {
            logger.error("The evaluated value {} was not a yaml Array compatible type and cannot be split.", new Object[]{yamlResult});
            processSession.transfer(original, REL_FAILURE);
            return;
        }

        List resultList = (List) yamlResult;

        Map<String, String> attributes = new HashMap<>();
        final String fragmentId = UUID.randomUUID().toString();
        attributes.put(FRAGMENT_ID.key(), fragmentId);
        attributes.put(FRAGMENT_COUNT.key(), Integer.toString(resultList.size()));

        for (int i = 0; i < resultList.size(); i++) {
            Object resultSegment = resultList.get(i);
            FlowFile split = processSession.create(original);
            split = processSession.write(split, (out) -> {
                        String output = yaml.dump(resultSegment);
                        //String yamlBegin = "---\n";
                        byte[] sourceByte = output.getBytes(StandardCharsets.UTF_8);
                        //out.write(yamlBegin.getBytes(StandardCharsets.UTF_8));
                        out.write(sourceByte);
                    }
            );
            attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), split.getAttribute(CoreAttributes.FILENAME.key()));
            attributes.put(FRAGMENT_INDEX.key(), Integer.toString(i));
            processSession.transfer(processSession.putAllAttributes(split, attributes), REL_SPLIT);
        }
        original = copyAttributesToOriginal(processSession, original, fragmentId, resultList.size());
        processSession.transfer(original, REL_ORIGINAL);
        logger.info("Split {} into {} FlowFiles", new Object[]{original, resultList.size()});

    }
}