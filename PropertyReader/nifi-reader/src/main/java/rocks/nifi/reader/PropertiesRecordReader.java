
package rocks.nifi.reader;


import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StringUtils;


import java.util.Enumeration;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.NoSuchElementException;

import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.RecordSchema;


public class PropertiesRecordReader implements RecordReader {
    private final ComponentLog logger;
    private final RecordSchema schema;

    private final Properties prop;
    private final Enumeration<?> enumeration;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;


    public PropertiesRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {
        this.logger = logger;
        this.schema = schema;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        prop = new Properties();
        prop.load(in);
        enumeration = prop.propertyNames();
    }


    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws MalformedRecordException {
        if (!enumeration.hasMoreElements()) {
            return null;
        }
        else {

            final Record record = parseRecord(enumeration.nextElement(), schema);
            if (record != null) {
                return record;
            } else {
                return new MapRecord(this.schema, Collections.EMPTY_MAP);
            }

        }

    }


    private Record parseRecord(Object nextElementKey, RecordSchema schema) throws MalformedRecordException {
        final Map<String, Object> recordValues = new HashMap<>();

        RecordField keyField = schema.getField(0);
        Optional<DataType> optKeyDatatype =  schema.getDataType(keyField.getFieldName());
        DataType keyDatatype;
        final Object keyData;
        try {
            keyDatatype = optKeyDatatype.get();
            if ((keyData = parseStringForType((String)nextElementKey, keyField.getFieldName(), keyDatatype)) != null) {
                recordValues.put(keyField.getFieldName(), keyData);
            }
        } catch (NoSuchElementException exc) {
            throw new MalformedRecordException("Error: cannot parse Properties", exc);
        }



        Object property = prop.getProperty((String)nextElementKey);
        RecordField valueField = schema.getField(1);
        Optional<DataType> optValueDatatype =  schema.getDataType(valueField.getFieldName());
        DataType valueDatatype;
        Object valueData;
        try {
            valueDatatype = optValueDatatype.get();
            if((valueData = parseStringForType((String)property, valueField.getFieldName(), valueDatatype)) != null) {
                recordValues.put(valueField.getFieldName(), valueData);
            }
        } catch (NoSuchElementException exc) {
            throw new MalformedRecordException("Error: cannot parse Properties", exc);
        }


        if (recordValues.size() > 0) {
            return new MapRecord(schema, recordValues);
        } else {
            return null;
        }
    }

    private Object parseStringForType(String data, String fieldName, DataType dataType) {
        switch (dataType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                return DataTypeUtils.convertType(data, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
            }
        }
        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() {}
}
