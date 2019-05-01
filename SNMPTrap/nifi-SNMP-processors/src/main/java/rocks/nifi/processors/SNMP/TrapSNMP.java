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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.AbstractTarget;
import org.snmp4j.ScopedPDU;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.AbstractVariable;
import org.snmp4j.smi.AssignableFromInteger;
import org.snmp4j.smi.AssignableFromLong;
import org.snmp4j.smi.AssignableFromString;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;

@Tags({"example"})
@CapabilityDescription("Provide a description")
public class TrapSNMP extends AbstractSNMPProcessor<SNMPTrapper> {

    public static final PropertyDescriptor OID = new PropertyDescriptor.Builder()
            .name("snmp-oid")
            .displayName("OID")
            .description("The OID to request")
            .required(true)
            .addValidator(SNMPUtils.SNMP_OID_VALIDATOR)
            .build();

    /** relationship for success */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that have been successfully used to perform SNMP Trap are routed to this relationship")
            .build();
    /** relationship for failure */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that failed during the SNMP Trap care routed to this relationship")
            .build();

    /** list of properties descriptors */
    private final static List<PropertyDescriptor> propertyDescriptors;

    /** list of relationships */
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * @see org.apache.nifi.snmp.processors.AbstractSNMPProcessor#onTriggerSnmp(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    protected void onTriggerSnmp(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            // Create the PDU object
            PDU pdu = null;
            if(this.snmpTarget.getVersion() == SnmpConstants.version3) {
                pdu = new ScopedPDU();
            } else {
                pdu = new PDU();
            }
            if(this.addVariables(pdu, flowFile.getAttributes())) {
                if(this.snmpTarget.getVersion() == SnmpConstants.version1) {
                    pdu.setType(PDU.V1TRAP);
                }
                else {
                    pdu.setType(PDU.TRAP);
                }
                try {
                    this.targetResource.trap(pdu);
                } catch (IOException e) {
                    processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                    this.getLogger().error("Failed while executing SNMP Trap via " + this.targetResource, e);
                    context.yield();
                    return;
                }
            } else {
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                this.getLogger().warn("No attributes found in the FlowFile to perform SNMP Trap");
                return;
            }
            processSession.transfer(flowFile, REL_SUCCESS);
        }
    }

    /**
     * Method to construct {@link VariableBinding} based on {@link FlowFile}
     * attributes in order to update the {@link PDU} that is going to be sent to
     * the SNMP Agent.
     * @param pdu {@link PDU} to be sent
     * @param attributes {@link FlowFile} attributes
     * @return true if at least one {@link VariableBinding} has been created, false otherwise
     */
    private boolean addVariables(PDU pdu, Map<String, String> attributes) {
        boolean result = false;
        for (Map.Entry<String, String> attributeEntry : attributes.entrySet()) {
            if (attributeEntry.getKey().startsWith(SNMPUtils.SNMP_PROP_PREFIX)) {
                String[] splits = attributeEntry.getKey().split("\\" + SNMPUtils.SNMP_PROP_DELIMITER);
                String snmpPropName = splits[1];
                String snmpPropValue = attributeEntry.getValue();
                if(SNMPUtils.OID_PATTERN.matcher(snmpPropName).matches()) {
                    Variable var = null;
                    if (splits.length == 2) { // no SMI syntax defined
                        var = new OctetString(snmpPropValue);
                    } else {
                        int smiSyntax = Integer.valueOf(splits[2]);
                        var = this.stringToVariable(snmpPropValue, smiSyntax);
                    }
                    if(var != null) {
                        VariableBinding varBind = new VariableBinding(new OID(snmpPropName), var);
                        pdu.add(varBind);
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Method to create the variable from the attribute value and the given SMI syntax value
     * @param value attribute value
     * @param smiSyntax attribute SMI Syntax
     * @return variable
     */
    private Variable stringToVariable(String value, int smiSyntax) {
        Variable var = AbstractVariable.createFromSyntax(smiSyntax);
        try {
            if (var instanceof AssignableFromString) {
                ((AssignableFromString) var).setValue(value);
            } else if (var instanceof AssignableFromInteger) {
                ((AssignableFromInteger) var).setValue(Integer.valueOf(value));
            } else if (var instanceof AssignableFromLong) {
                ((AssignableFromLong) var).setValue(Long.valueOf(value));
            } else {
                this.getLogger().error("Unsupported conversion of [" + value +"] to " + var.getSyntaxString());
                var = null;
            }
        } catch (IllegalArgumentException e) {
            this.getLogger().error("Unsupported conversion of [" + value +"] to " + var.getSyntaxString(), e);
            var = null;
        }
        return var;
    }

    /**
     * @see org.apache.nifi.components.AbstractConfigurableComponent#getSupportedPropertyDescriptors()
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     * @see org.apache.nifi.processor.AbstractSessionFactoryProcessor#getRelationships()
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * @see org.apache.nifi.snmp.processors.AbstractSNMPProcessor#finishBuildingTargetResource(org.apache.nifi.processor.ProcessContext)
     */
    @Override
    protected SNMPTrapper finishBuildingTargetResource(ProcessContext context) {
        String oid = context.getProperty(OID).getValue();
        return new SNMPTrapper(this.snmp, this.snmpTarget, new OID(oid));
    }
}
