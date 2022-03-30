package org.apache.iceberg.flink;

import java.util.Arrays;
import java.util.Objects;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.apache.iceberg.Schema;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class GenericRowDataWithSchema implements RowData {

    /**
     * The array to store the actual internal format values.
     */
    private final Object[] fields;

    /***
     * Optionally,but required when length of fields is different from the target table
     */
    private final Schema schema;

    /**
     * The kind of change that a row describes in a changelog.
     */
    private RowKind kind;

    /**
     * Creates an instance of {@link GenericRowDataWithSchema} with given kind and number of fields.
     *
     * <p>Initially, all fields are set to null.
     *
     * <p>Note: All fields of the row must be internal data structures.
     *
     * @param kind  kind of change that this row describes in a changelog
     * @param arity number of fields
     */
    public GenericRowDataWithSchema(RowKind kind, int arity) {
        this.fields = new Object[arity];
        this.schema=null;
        this.kind = kind;
    }

    public GenericRowDataWithSchema(RowKind kind, Schema schema) {
        this.schema = schema;
        this.fields = new Object[schema.columns().size()];
        this.kind = kind;
    }

    /**
     * Creates an instance of {@link GenericRowDataWithSchema} with given number of fields.
     *
     * <p>Initially, all fields are set to null. By default, the row describes a {@link
     * RowKind#INSERT} in a changelog.
     *
     * <p>Note: All fields of the row must be internal data structures.
     *
     * @param arity number of fields
     */
    public GenericRowDataWithSchema(int arity) {
        this.fields = new Object[arity];
        this.schema=null;
        this.kind = RowKind.INSERT; // INSERT as default
    }

    public GenericRowDataWithSchema(Schema schema) {
        this.fields = new Object[schema.columns().size()];
        this.schema=schema;
        this.kind = RowKind.INSERT; // INSERT as default
    }

    /**
     * Creates an instance of {@link GenericRowDataWithSchema} with given field values.
     *
     * <p>By default, the row describes a {@link RowKind#INSERT} in a changelog.
     *
     * <p>Note: All fields of the row must be internal data structures.
     */
    public static GenericRowDataWithSchema of(Object... values) {
        GenericRowDataWithSchema row = new GenericRowDataWithSchema(values.length);
//        TypeInformation typeInfo = TypeExtractor.getForObject(values[0]);
        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }
        return row;
    }

    public static GenericRowDataWithSchema of(Schema schema,Object... values) {
        GenericRowDataWithSchema row = new GenericRowDataWithSchema(schema);
        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }
        return row;
    }

    /**
     * Sets the field value at the given position.
     *
     * <p>Note: The given field value must be an internal data structures. Otherwise the {@link
     * GenericRowDataWithSchema} is corrupted and may throw exception when processing. See {@link RowData} for
     * more information about internal data structures.
     *
     * <p>The field value can be null for representing nullability.
     */
    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    /**
     * Returns the field value at the given position.
     *
     * <p>Note: The returned value is in internal data structure. See {@link RowData} for more
     * information about internal data structures.
     *
     * <p>The returned field value can be null for representing nullability.
     */
    public Object getField(int pos) {
        return this.fields[pos];
    }

    @Override
    public int getArity() {
        return fields.length;
    }

    @Override
    public RowKind getRowKind() {
        return kind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        checkNotNull(kind);
        this.kind = kind;
    }

    public Schema getSchema() {
       return schema;
    }


    @Override
    public boolean isNullAt(int pos) {
        return this.fields[pos] == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) this.fields[pos];
    }

    @Override
    public byte getByte(int pos) {
        return (byte) this.fields[pos];
    }

    @Override
    public short getShort(int pos) {
        return (short) this.fields[pos];
    }

    @Override
    public int getInt(int pos) {
        return (int) this.fields[pos];
    }

    @Override
    public long getLong(int pos) {
        return (long) this.fields[pos];
    }

    @Override
    public float getFloat(int pos) {
        return (float) this.fields[pos];
    }

    @Override
    public double getDouble(int pos) {
        return (double) this.fields[pos];
    }

    @Override
    public StringData getString(int pos) {
        return (StringData) this.fields[pos];
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return (DecimalData) this.fields[pos];
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return (TimestampData) this.fields[pos];
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return (RawValueData<T>) this.fields[pos];
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) this.fields[pos];
    }

    @Override
    public ArrayData getArray(int pos) {
        return (ArrayData) this.fields[pos];
    }

    @Override
    public MapData getMap(int pos) {
        return (MapData) this.fields[pos];
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return (RowData) this.fields[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GenericRowDataWithSchema)) {
            return false;
        }
        GenericRowDataWithSchema that = (GenericRowDataWithSchema) o;
        return kind == that.kind && Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(kind);
        result = 31 * result + Arrays.deepHashCode(fields);
        return result;
    }

    // ----------------------------------------------------------------------------------------
    // Utilities
    // ----------------------------------------------------------------------------------------

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(kind.shortString()).append("(");
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(StringUtils.arrayAwareToString(fields[i]));
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Creates an instance of {@link GenericRowDataWithSchema} with given kind and field values.
     *
     * <p>Note: All fields of the row must be internal data structures.
     */
    public static GenericRowDataWithSchema ofKind(RowKind kind, Object... values) {
    GenericRowDataWithSchema row = new GenericRowDataWithSchema(kind, values.length);

        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }

        return row;
    }
}
