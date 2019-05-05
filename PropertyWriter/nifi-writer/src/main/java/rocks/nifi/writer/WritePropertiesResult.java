
package rocks.nifi.writer;


import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.MalformedRecordException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;
import java.util.Set;
import java.util.List;

public class WritePropertiesResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {

    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;

    private final Object[] fieldValues;
    private String[] fieldNames;
    private final Properties prop;

    public WritePropertiesResult(final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter, final OutputStream out,
                          final String dateFormat, final String timeFormat, final String timestampFormat) {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;


        final OutputStreamWriter streamWriter = new OutputStreamWriter(out);

        fieldValues = new Object[recordSchema.getFieldCount()];
        prop = new Properties();
    }

    private String getFormat(final RecordField field) {
        final DataType dataType = field.getDataType();
        switch (dataType.getFieldType()) {
            case DATE:
                return dateFormat;
            case TIME:
                return timeFormat;
            case TIMESTAMP:
                return timestampFormat;
        }

        return dataType.getFormat();
    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        schemaWriter.writeHeader(recordSchema, getOutputStream());
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public void close() {

    }

    @Override
    public void flush() {

    }

    private String[] getFieldNames(final Record record) {
        if (fieldNames != null) {
            return fieldNames;
        }

        final Set<String> allFields = new LinkedHashSet<>();
        allFields.addAll(record.getRawFieldNames());
        allFields.addAll(recordSchema.getFieldNames());
        fieldNames = allFields.toArray(new String[0]);
        return fieldNames;
    }


    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }

        if(recordSchema.getFields().size()==0) {
            prop.setProperty("", "");
        }
        else if(recordSchema.getFields().size()==1) {
            RecordField recordField = recordSchema.getField(0);
            fieldValues[0] = record.getAsString(recordField, getFormat(recordField));
            if(fieldValues[0]!=null) {
                prop.setProperty((String)fieldValues[0], "");
            }
            else {
                prop.setProperty("", "");
            }
        }
        else {
            RecordField recordField;
            for (int i=0; i<2; ++i) {
                recordField = recordSchema.getField(i);
                fieldValues[i] = record.getAsString(recordField, getFormat(recordField));
                if(fieldValues[i]==null) {
                    fieldValues[i]="";
                }
            }

            prop.setProperty((String)fieldValues[0], (String)fieldValues[1]);
        }

        prop.store(getOutputStream(), "");
        prop.clear();
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }


        final String[] fieldNames = getFieldNames(record);
        final Object[] recordFieldValues = (fieldNames.length == this.fieldValues.length) ? this.fieldValues : new String[fieldNames.length];


        int i = 0;
        for (final String fieldName : fieldNames) {
            final Optional<RecordField> recordField = recordSchema.getField(fieldName);
            if (recordField.isPresent()) {
                recordFieldValues[i++] = record.getAsString(fieldName, getFormat(recordField.get()));
            } else {
                recordFieldValues[i++] = record.getAsString(fieldName);
            }
        }


        if(recordFieldValues.length==0) {
            prop.setProperty("", "");
        }
        else if(recordFieldValues.length==1) {
            prop.setProperty((String)recordFieldValues[0], "");
        }
        else {
            prop.setProperty((String)recordFieldValues[0], (String)recordFieldValues[1]);
        }
        prop.store(getOutputStream(), "");
        prop.clear();
        final Map<String, String> attributes = schemaWriter.getAttributes(recordSchema);
        return WriteResult.of(incrementRecordCount(), attributes);
    }

    @Override
    public String getMimeType() {
        return "text/properties";
    }
}
