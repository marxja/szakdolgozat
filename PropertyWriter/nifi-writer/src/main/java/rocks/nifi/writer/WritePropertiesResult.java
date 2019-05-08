
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

    private final String key;
    private final String value;

    public WritePropertiesResult(final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter, final OutputStream out,
                          final String dateFormat, final String timeFormat, final String timestampFormat, final String key, final String value) {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;

        this.key = key;
        this.value = value;

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

        if(key.equals("FirstField") && value.equals("SecondField")) {
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
        }
        else if(!key.equals("FirstField") && value.equals("SecondField")) {
            String firstValue;
            String secondValue;
            Optional<RecordField> optRecordField = recordSchema.getField(key);
            if(optRecordField.isPresent()) {
                RecordField recordField = optRecordField.get();
                firstValue = record.getAsString(recordField, getFormat(recordField));
                if(firstValue == null) {
                    firstValue = "";
                }
            }
            else {
                firstValue = "";
            }
            if(recordSchema.getFields().size()<2) {
                secondValue = "";
            }
            else {
                RecordField recordField = recordSchema.getField(1);
                secondValue = record.getAsString(recordField, getFormat(recordField));
                if(secondValue==null) {
                    secondValue="";
                }
            }

            prop.setProperty(firstValue, secondValue);
        }
        else if(key.equals("FirstField") && !value.equals("SecondField")) {
            String firstValue;
            String secondValue;
            if(recordSchema.getFields().size()<1) {
                firstValue = "";
            }
            else {
                RecordField recordField = recordSchema.getField(0);
                firstValue = record.getAsString(recordField, getFormat(recordField));
                if(firstValue==null) {
                    firstValue="";
                }
            }

            Optional<RecordField> optRecordField = recordSchema.getField(value);
            if(optRecordField.isPresent()) {
                RecordField recordField = optRecordField.get();
                secondValue = record.getAsString(recordField, getFormat(recordField));
                if(secondValue == null) {
                    secondValue = "";
                }
            }
            else {
                secondValue = "";
            }

            prop.setProperty(firstValue, secondValue);
        }
        else {
            String firstValue;
            String secondValue;
            Optional<RecordField> optFirstRecordField = recordSchema.getField(key);
            if(optFirstRecordField.isPresent()) {
                RecordField recordField = optFirstRecordField.get();
                firstValue = record.getAsString(recordField, getFormat(recordField));
                if(firstValue == null) {
                    firstValue = "";
                }
            }
            else {
                firstValue = "";
            }
            Optional<RecordField> optSecondRecordField = recordSchema.getField(value);
            if(optSecondRecordField.isPresent()) {
                RecordField recordField = optSecondRecordField.get();
                secondValue = record.getAsString(recordField, getFormat(recordField));
                if(secondValue == null) {
                    secondValue = "";
                }
            }
            else {
                secondValue = "";
            }

            prop.setProperty(firstValue, secondValue);
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

        String firstValue=null;
        String secondValue=null;

        int i = 0;
        for (final String fieldName : fieldNames) {


            final Optional<RecordField> recordField = recordSchema.getField(fieldName);
            if (recordField.isPresent()) {
                recordFieldValues[i++] = record.getAsString(fieldName, getFormat(recordField.get()));
            } else {
                recordFieldValues[i++] = record.getAsString(fieldName);
            }
            if(!key.equals("FirstField") && key.equals(fieldName)) {
                firstValue = (String)recordFieldValues[i-1];
            }
            if(!value.equals("SecondField") && value.equals(fieldName)) {
                secondValue = (String)recordFieldValues[i-1];
            }
        }

        if(key.equals("FirstField") && value.equals("SecondField")) {
            if(recordFieldValues.length==0) {
                prop.setProperty("", "");
            }
            else if(recordFieldValues.length==1) {
                prop.setProperty((String)recordFieldValues[0], "");
            }
            else {
                prop.setProperty((String)recordFieldValues[0], (String)recordFieldValues[1]);
            }
        }
        else if(!key.equals("FirstField") && value.equals("SecondField")) {
            if(firstValue==null) {
                firstValue = "";
            }
            if(recordFieldValues.length==0) {
                prop.setProperty("", "");
            }
            else if(recordFieldValues.length==1) {
                prop.setProperty(firstValue, "");
            }
            else {
                prop.setProperty(firstValue, (String)recordFieldValues[1]);
            }
        }
        else if(key.equals("FirstField") && !value.equals("SecondField")) {
            if(secondValue==null) {
                secondValue = "";
            }
            if(recordFieldValues.length==0) {
                prop.setProperty("", "");
            }
            else {
                prop.setProperty((String)recordFieldValues[0], secondValue);
            }
        }
        else {
            if(firstValue==null) {
                firstValue = "";
            }
            if(secondValue==null) {
                secondValue="";
            }
            prop.setProperty(firstValue, secondValue);
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
