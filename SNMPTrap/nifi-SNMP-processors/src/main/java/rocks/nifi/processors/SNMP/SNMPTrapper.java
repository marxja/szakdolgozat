package rocks.nifi.processors.SNMP;

import java.io.IOException;

import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.AbstractTarget;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.ScopedPDU;
import org.snmp4j.Snmp;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.VariableBinding;

public class SNMPTrapper extends SNMPWorker {
    /** logger */
    private final static Logger logger = LoggerFactory.getLogger(SNMPTrapper.class);


    private final OID oid;


    /**
     * Creates an instance of this setter
     * @param snmp instance of {@link Snmp}
     * @param target instance of {@link AbstractTarget} to request
     */
    SNMPTrapper(Snmp snmp, AbstractTarget target, OID oid) {
        super(snmp, target);
        this.oid = oid;
        logger.info("Successfully initialized SNMP Trapper");
    }

    /**
     * Executes the SNMP set request and returns the response
     * @param pdu PDU to send
     * @return Response event
     * @throws IOException IO Exception
     */
    public void trap(PDU pdu) throws IOException {
        try {
            pdu.add(new VariableBinding(this.oid));
            if(this.target.getVersion() == SnmpConstants.version1) {
                this.snmp.trap((PDUv1)pdu, this.target);
            }
            else {
                this.snmp.notify(pdu, this.target);
            }
        } catch (IOException e) {
            logger.error("Failed to trap" + this, e);
            throw new ProcessException(e);
        }
    }
}
