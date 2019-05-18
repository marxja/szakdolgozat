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
import java.lang.Exception;
import java.lang.NullPointerException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.AbstractProcessor;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.AbstractTarget;
import org.snmp4j.TransportMapping;
import org.snmp4j.ScopedPDU;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.AbstractVariable;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.CommunityTarget;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.AssignableFromInteger;
import org.snmp4j.PDUv1;
import org.snmp4j.smi.AssignableFromLong;
import org.snmp4j.smi.AssignableFromString;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;

import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.mp.MPv3;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.UserTarget;
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.Priv3DES;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivAES192;
import org.snmp4j.security.PrivAES256;
import org.snmp4j.security.PrivDES;



@Tags({"example"})
@CapabilityDescription("Provide a description")
public class TrapSNMP extends AbstractProcessor {

    /** property to define host of the SNMP manager */
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("snmp-hostname")
            .displayName("Host Name")
            .description("Network address of SNMP Manager (e.g., localhost)")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** property to define port of the SNMP manager */
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("snmp-port")
            .displayName("Port")
            .description("Numeric value identifying Port of SNMP Manager (e.g., 162)")
            .required(true)
            .defaultValue("162")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    /** property to define SNMP version to use */
    public static final PropertyDescriptor SNMP_VERSION = new PropertyDescriptor.Builder()
            .name("snmp-version")
            .displayName("SNMP Version")
            .description("SNMP Version to use")
            .required(true)
            .allowableValues("SNMPv1", "SNMPv2c", "SNMPv3")
            .defaultValue("SNMPv1")
            .build();


	 public static final PropertyDescriptor SNMP_COMMUNITY = new PropertyDescriptor.Builder()
            .name("snmp-community")
            .displayName("SNMP Community (v1 & v2c)")
            .description("SNMP Community to use (e.g., public)")
            .required(false)
            .defaultValue("public")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

	/** property to define SNMP security level to use */
    public static final PropertyDescriptor SNMP_SECURITY_LEVEL = new PropertyDescriptor.Builder()
            .name("snmp-security-level")
            .displayName("SNMP Security Level")
            .description("SNMP Security Level to use")
            .required(true)
            .allowableValues("noAuthNoPriv", "authNoPriv", "authPriv")
            .defaultValue("authPriv")
            .build();

    /** property to define SNMP security name to use */
    public static final PropertyDescriptor SNMP_SECURITY_NAME = new PropertyDescriptor.Builder()
            .name("snmp-security-name")
            .displayName("SNMP Security name / user name")
            .description("Security name used for SNMP exchanges")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** property to define SNMP authentication protocol to use */
    public static final PropertyDescriptor SNMP_AUTH_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-authentication-protocol")
            .displayName("SNMP Authentication Protocol")
            .description("SNMP Authentication Protocol to use")
            .required(true)
            .allowableValues("MD5", "SHA", "")
            .defaultValue("")
            .build();

    /** property to define SNMP authentication password to use */
    public static final PropertyDescriptor SNMP_AUTH_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-authentication-passphrase")
            .displayName("SNMP Authentication pass phrase")
            .description("Pass phrase used for SNMP authentication protocol")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    /** property to define SNMP private protocol to use */
    public static final PropertyDescriptor SNMP_PRIV_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol")
            .displayName("SNMP Private Protocol")
            .description("SNMP Private Protocol to use")
            .required(true)
            .allowableValues("DES", "3DES", "AES128", "AES192", "AES256", "")
            .defaultValue("")
            .build();

    /** property to define SNMP private password to use */
    public static final PropertyDescriptor SNMP_PRIV_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol-passphrase")
            .displayName("SNMP Private protocol pass phrase")
            .description("Pass phrase used for SNMP private protocol")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();


    /** property to define the number of SNMP retries when requesting the SNMP Agent */
    public static final PropertyDescriptor SNMP_RETRIES = new PropertyDescriptor.Builder()
            .name("snmp-retries")
            .displayName("Number of retries")
            .description("Set the number of retries when requesting the SNMP Manager")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    /** property to define the timeout when requesting the SNMP Agent */
    public static final PropertyDescriptor SNMP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("snmp-timeout")
            .displayName("Timeout (ms)")
            .description("Set the timeout (in milliseconds) when requesting the SNMP Manager")
            .required(true)
            .defaultValue("5000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
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


    private final static Pattern OID_PATTERN = Pattern.compile("[[0-9]+\\.]*");

    /** SNMP */
    private volatile Snmp snmp;

    
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HOST);
        _propertyDescriptors.add(PORT);
        _propertyDescriptors.add(SNMP_VERSION);
        _propertyDescriptors.add(SNMP_COMMUNITY);
        _propertyDescriptors.add(SNMP_SECURITY_LEVEL);
        _propertyDescriptors.add(SNMP_SECURITY_NAME);
        _propertyDescriptors.add(SNMP_AUTH_PROTOCOL);
        _propertyDescriptors.add(SNMP_AUTH_PASSWORD);
        _propertyDescriptors.add(SNMP_PRIV_PROTOCOL);
        _propertyDescriptors.add(SNMP_PRIV_PASSWORD);
        _propertyDescriptors.add(SNMP_RETRIES);
        _propertyDescriptors.add(SNMP_TIMEOUT);
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
    public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            String snmpVersion = context.getProperty(SNMP_VERSION).getValue();
            int version=0;
            switch (snmpVersion) {
                case "SNMPv3":
					version = SnmpConstants.version3;
					break;
                case "SNMPv2c":
                    version = SnmpConstants.version2c;
                    break;
                case "SNMPv1":
                default:
                    version = SnmpConstants.version1;
                    break;
            }
	    
			if(version == SnmpConstants.version3) {
				try {
					TransportMapping transport = new DefaultUdpTransportMapping();
					snmp = new Snmp(transport);
				} catch (IOException e) {
					this.getLogger().error("socket binding fails" + this, e);
					processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
					throw new ProcessException(e);
				}   
				  
				USM usm = new USM(SecurityProtocols.getInstance(),
					new OctetString(MPv3.createLocalEngineID()), 0);
				SecurityProtocols.getInstance().addPrivacyProtocol(new PrivAES192());
				SecurityProtocols.getInstance().addPrivacyProtocol(new PrivAES256());
				SecurityProtocols.getInstance().addPrivacyProtocol(new Priv3DES());
				SecurityModels.getInstance().addSecurityModel(usm);    
				  
				//transport.listen();     
				  
				final String username = context.getProperty(SNMP_SECURITY_NAME).getValue();
				final String authPassword = context.getProperty(SNMP_AUTH_PASSWORD).getValue();
				final String privPassword = context.getProperty(SNMP_PRIV_PASSWORD).getValue();
				final String authProtocol = context.getProperty(SNMP_AUTH_PROTOCOL).getValue();
				final String privProtocol = context.getProperty(SNMP_PRIV_PROTOCOL).getValue();
				OctetString aPwd = authPassword != null ? new OctetString(authPassword) : null;
				OctetString pPwd = privPassword != null ? new OctetString(privPassword) : null;
				  
				this.snmp.getUSM().addUser(new OctetString(username),
                    new UsmUser(new OctetString(username), getAuth(authProtocol), aPwd, getPriv(privProtocol), pPwd));				  
			 
				// Create Target
				UserTarget target = new UserTarget();
				target.setAddress(new UdpAddress(context.getProperty(HOST).getValue() + "/" + context.getProperty(PORT).getValue()));
				target.setRetries(context.getProperty(SNMP_RETRIES).asInteger());
				target.setTimeout(context.getProperty(SNMP_TIMEOUT).asInteger());
				target.setVersion(SnmpConstants.version3);
				target.setSecurityLevel(getSecLevel(context.getProperty(SNMP_SECURITY_LEVEL).getValue()));
				target.setSecurityName(new OctetString(context.getProperty(SNMP_SECURITY_NAME).getValue()));

				// Create PDU for V3
				ScopedPDU pdu = new ScopedPDU();
				/*pdu.setRequestID(new Integer32(1234));
				pdu.add(new VariableBinding(SnmpConstants.sysUpTime));
				pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID,
				 SnmpConstants.linkDown));*/

				if(this.addVariables(pdu, flowFile.getAttributes())) {
					pdu.setType(ScopedPDU.NOTIFICATION);
				} else {
					processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
					this.getLogger().warn("No attributes found in the FlowFile to perform SNMP Trap");
					return;
				}

				try {
					snmp.send(pdu, target);
				} catch (IOException e) {
					this.getLogger().error("Failed to trap" + this, e);
					processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
					throw new ProcessException(e);
				}
			}
			else {
				PDU pdu = new PDU();
				
				if(this.addVariables(pdu, flowFile.getAttributes())) {
					if(version == SnmpConstants.version1) {
						pdu.setType(PDU.V1TRAP);
					}
					else {
						pdu.setType(PDU.TRAP);
					}

				} else {
					processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
					this.getLogger().warn("No attributes found in the FlowFile to perform SNMP Trap");
					return;
				}
				
				try {
					TransportMapping transport = new DefaultUdpTransportMapping();
					snmp = new Snmp(transport);
				} catch (IOException e) {
					this.getLogger().error("socket binding fails" + this, e);
					processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
					throw new ProcessException(e);
				}
				
				
				//Create target
				CommunityTarget comtarget = new CommunityTarget();	
				comtarget.setVersion(version);
				comtarget.setAddress(new UdpAddress(context.getProperty(HOST).getValue() + "/" + context.getProperty(PORT).getValue()));
				comtarget.setRetries(context.getProperty(SNMP_RETRIES).asInteger());
				comtarget.setTimeout(context.getProperty(SNMP_TIMEOUT).asInteger());
				
				String community = context.getProperty(SNMP_COMMUNITY).getValue();
				if(community != null) {
					comtarget.setCommunity(new OctetString(community));
				}
				
		
				//Send
				try {
					snmp.send(pdu, comtarget);
				} catch (IOException e) {
					this.getLogger().error("Failed to trap" + this, e);
					processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
					throw new ProcessException(e);
				}
			}
	    
            
			
            processSession.transfer(flowFile, REL_SUCCESS);
        }
    }

    private boolean addVariables(PDU pdu, Map<String, String> attributes) {
        boolean result = false;
        for (Map.Entry<String, String> attributeEntry : attributes.entrySet()) {
            if (attributeEntry.getKey().startsWith("snmp$")) {
                String[] splits = attributeEntry.getKey().split("\\" + "$");
                String snmpPropName = splits[1];
                String snmpPropValue = attributeEntry.getValue();
                if(OID_PATTERN.matcher(snmpPropName).matches()) {
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

 
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

	public static OID getPriv(String privProtocol) {
        switch (privProtocol) {
        case "DES":
            return PrivDES.ID;
        case "3DES":
            return Priv3DES.ID;
        case "AES128":
            return PrivAES128.ID;
        case "AES192":
            return PrivAES192.ID;
        case "AES256":
            return PrivAES256.ID;
        default:
            return null;
        }
    }


    public static OID getAuth(String authProtocol) {
        switch (authProtocol) {
        case "SHA":
            return AuthSHA.ID;
        case "MD5":
            return AuthMD5.ID;
        default:
            return null;
        }
    }
    
    
    public static int getSecLevel(String level) {
        switch (level) {
        case "noAuthNoPriv":
            return SecurityLevel.NOAUTH_NOPRIV;
        case "authNoPriv":
            return SecurityLevel.AUTH_NOPRIV;
        case "authPriv":
        default:
            return SecurityLevel.AUTH_PRIV;
        }
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
   
}
