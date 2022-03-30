/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.GenericRowDataWithSchema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;


class IcebergStreamWriter<T> extends AbstractStreamOperator<WriteResult>
        implements OneInputStreamOperator<T, WriteResult>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private TableLoader tableLoader;
    private Table table;
    private TaskWriterFactory<T> taskWriterFactory;

    private transient TaskWriter<T> writer;
    private transient int subTaskId;
    private transient int attemptId;

    private transient List<Integer> equalityFieldIds;
    private transient boolean upsert;

    IcebergStreamWriter(TableLoader tableLoader, List<Integer> equalityFieldIds,
                        boolean upsert) {
        this.tableLoader = tableLoader;
        this.equalityFieldIds=equalityFieldIds;
        this.upsert=upsert;
        tableLoader.open();
        this.table = tableLoader.loadTable();
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
        if (requestedSchema != null) {
            // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing iceberg schema.
            Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
            TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

            // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will be promoted to
            // iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT (backend by 1 'byte'), we will
            // read 4 bytes rather than 1 byte, it will mess up the byte array in BinaryRowData. So here we must use flink
            // schema.
            return (RowType) requestedSchema.toRowDataType().getLogicalType();
        } else {
            return FlinkSchemaUtil.convert(schema);
        }
    }

    TaskWriter newWriter(TableLoader tableLoader) {
        tableLoader.open();
        Table table = tableLoader.loadTable();
        // Convert the requested flink table schema to flink row type.
        RowType flinkRowType = toFlinkRowType(table.schema(), null);
        // Initialize the task writer factory.
        Map<String, String> props = table.properties();
        long targetFileSize = getTargetFileSizeBytes(props);
        FileFormat fileFormat = getFileFormat(props);

        TaskWriterFactory taskWriterFactory = new RowDataTaskWriterFactory(
                table, flinkRowType, targetFileSize,
                fileFormat, equalityFieldIds,upsert);
        this.taskWriterFactory = taskWriterFactory;
        this.taskWriterFactory.initialize(subTaskId, attemptId);

        // Initialize the task writer.
        TaskWriter writer = taskWriterFactory.create();
        return writer;
    }

    @Override
    public void open() {
        this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.attemptId = getRuntimeContext().getAttemptNumber();
        this.writer = newWriter(tableLoader);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // close all open files and emit files to downstream committer operator
        emit(writer.complete());
        this.writer = newWriter(tableLoader);
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        GenericRowDataWithSchema rowData = (GenericRowDataWithSchema) element.getValue();
        Schema tableSchema = table.schema();
        Schema rowSchema = rowData.getSchema();
        int tableColumnSize = tableSchema.columns().size();
        if (rowSchema != null && !tableSchema.sameSchema(rowSchema)) {
            int newColumn = rowData.getSchema().columns().size() - tableSchema.columns().size();
            //只增不减
            for (int col = 0; col < newColumn; col++) {
                //Operation updateSchema is not supported after the table is serialized
                tableLoader.open();
                Table table = tableLoader.loadTable();
                Types.NestedField field = rowData.getSchema().columns().get(tableColumnSize + col);
                table.updateSchema().addColumn(field.name(), field.type()).commit();
                table.refresh();
                this.table = table;
                emit(writer.complete());
                this.writer = newWriter(tableLoader);
            }
            GenericRowDataWithSchema newRowData = new GenericRowDataWithSchema(this.table.schema().columns().size());
            for (int pos = 0; pos < rowData.getArity(); pos++) {
                newRowData.setField(pos, rowData.getField(pos));
            }
            this.writer.write((T) newRowData);
        } else if (rowSchema == null && rowData.getArity() != tableSchema.columns().size()) {
            throw new IllegalArgumentException(
                    "When Row Schema is not provided,Row data size must match column size of the target table");
        } else {
            this.writer.write(element.getValue());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    @Override
    public void endInput() throws IOException {
        // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the remaining
        // completed files to downstream before closing the writer so that we won't miss any of them.
        emit(writer.complete());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("table_name", table.name())
                .add("subtask_id", subTaskId)
                .add("attempt_id", attemptId)
                .toString();
    }

    private void emit(WriteResult result) {
        output.collect(new StreamRecord<>(result));
    }

    private FileFormat getFileFormat(Map<String, String> properties) {
        String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    }

    private long getTargetFileSizeBytes(Map<String, String> properties) {
        return PropertyUtil.propertyAsLong(
                properties,
                WRITE_TARGET_FILE_SIZE_BYTES,
                WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    }
}