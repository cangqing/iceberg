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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.UPSERT_ENABLED;
import static org.apache.iceberg.TableProperties.UPSERT_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class FlinkSink {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSink.class);

  private static final String ICEBERG_STREAM_WRITER_NAME = IcebergStreamWriter.class.getSimpleName();
  private static final String ICEBERG_FILES_COMMITTER_NAME = IcebergFilesCommitter.class.getSimpleName();

  private FlinkSink() {
  }

  /**
   * Initialize a {@link Builder} to export the data from generic input data stream into iceberg table. We use
   * {@link RowData} inside the sink connector, so users need to provide a mapper function and a
   * {@link TypeInformation} to convert those generic records to a RowData DataStream.
   *
   * @param input      the generic source input data stream.
   * @param mapper     function to convert the generic data to {@link RowData}
   * @param outputType to define the {@link TypeInformation} for the input data.
   * @param <T>        the data type of records.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static <T> Builder builderFor(DataStream<T> input,
                                       MapFunction<T, RowData> mapper,
                                       TypeInformation<RowData> outputType) {
    return new Builder().forMapperOutputType(input, mapper, outputType);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link Row}s into iceberg table. We use
   * {@link RowData} inside the sink connector, so users need to provide a {@link TableSchema} for builder to convert
   * those {@link Row}s to a {@link RowData} DataStream.
   *
   * @param input       the source input data stream with {@link Row}s.
   * @param tableSchema defines the {@link TypeInformation} for input data.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRow(DataStream<Row> input, TableSchema tableSchema) {
    RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
    DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();

    DataFormatConverters.RowConverter rowConverter = new DataFormatConverters.RowConverter(fieldDataTypes);
    return builderFor(input, rowConverter::toInternal, FlinkCompatibilityUtil.toTypeInfo(rowType))
        .tableSchema(tableSchema);
  }

  /**
   * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s into iceberg table.
   *
   * @param input the source input data stream with {@link RowData}s.
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData(DataStream<RowData> input) {
    return new Builder().forRowData(input);
  }

  public static class Builder {
    private Function<String, DataStream<RowData>> inputCreator = null;
    private TableLoader tableLoader;
    private Table table;
    private TableSchema tableSchema;
    private boolean overwrite = false;
    private DistributionMode distributionMode = null;
    private Integer writeParallelism = null;
    private boolean upsert = false;
    private List<String> equalityFieldColumns = null;
    private String uidPrefix = null;

    private Builder() {
    }

    private Builder forRowData(DataStream<RowData> newRowDataInput) {
      this.inputCreator = ignored -> newRowDataInput;
      return this;
    }

    private <T> Builder forMapperOutputType(DataStream<T> input,
                                            MapFunction<T, RowData> mapper,
                                            TypeInformation<RowData> outputType) {
      this.inputCreator = newUidPrefix -> {
        if (newUidPrefix != null) {
          return input.map(mapper, outputType)
              .name(operatorName(newUidPrefix))
              .uid(newUidPrefix + "-mapper");
        } else {
          return input.map(mapper, outputType);
        }
      };
      return this;
    }

    /**
     * This iceberg {@link Table} instance is used for initializing {@link IcebergStreamWriter} which will write all
     * the records into {@link DataFile}s and emit them to downstream operator. Providing a table would avoid so many
     * table loading from each separate task.
     *
     * @param newTable the loaded iceberg table instance.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder table(Table newTable) {
      this.table = newTable;
      return this;
    }

    /**
     * The table loader is used for loading tables in {@link IcebergFilesCommitter} lazily, we need this loader because
     * {@link Table} is not serializable and could not just use the loaded table from Builder#table in the remote task
     * manager.
     *
     * @param newTableLoader to load iceberg table inside tasks.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder tableLoader(TableLoader newTableLoader) {
      this.tableLoader = newTableLoader;
      return this;
    }

    public Builder tableSchema(TableSchema newTableSchema) {
      this.tableSchema = newTableSchema;
      return this;
    }

    public Builder overwrite(boolean newOverwrite) {
      this.overwrite = newOverwrite;
      return this;
    }

    /**
     * Configure the write {@link DistributionMode} that the flink sink will use. Currently, flink support
     * {@link DistributionMode#NONE} and {@link DistributionMode#HASH}.
     *
     * @param mode to specify the write distribution mode.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder distributionMode(DistributionMode mode) {
      Preconditions.checkArgument(!DistributionMode.RANGE.equals(mode),
          "Flink does not support 'range' write distribution mode now.");
      this.distributionMode = mode;
      return this;
    }

    /**
     * Configuring the write parallel number for iceberg stream writer.
     *
     * @param newWriteParallelism the number of parallel iceberg stream writer.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder writeParallelism(int newWriteParallelism) {
      this.writeParallelism = newWriteParallelism;
      return this;
    }

    /**
     * All INSERT/UPDATE_AFTER events from input stream will be transformed to UPSERT events, which means it will
     * DELETE the old records and then INSERT the new records. In partitioned table, the partition fields should be
     * a subset of equality fields, otherwise the old row that located in partition-A could not be deleted by the
     * new row that located in partition-B.
     *
     * @param enabled indicate whether it should transform all INSERT/UPDATE_AFTER events to UPSERT.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder upsert(boolean enabled) {
      this.upsert = enabled;
      return this;
    }

    /**
     * Configuring the equality field columns for iceberg table that accept CDC or UPSERT events.
     *
     * @param columns defines the iceberg table's key.
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder equalityFieldColumns(List<String> columns) {
      this.equalityFieldColumns = columns;
      return this;
    }

    /**
     * Set the uid prefix for FlinkSink operators. Note that FlinkSink internally consists of multiple operators (like
     * writer, committer, dummy sink etc.) Actually operator uid will be appended with a suffix like "uidPrefix-writer".
     * <br><br>
     * If provided, this prefix is also applied to operator names.
     * <br><br>
     * Flink auto generates operator uid if not set explicitly. It is a recommended
     * <a href="https://ci.apache.org/projects/flink/flink-docs-master/docs/ops/production_ready/">
     * best-practice to set uid for all operators</a> before deploying to production. Flink has an option to {@code
     * pipeline.auto-generate-uid=false} to disable auto-generation and force explicit setting of all operator uid.
     * <br><br>
     * Be careful with setting this for an existing job, because now we are changing the operator uid from an
     * auto-generated one to this new value. When deploying the change with a checkpoint, Flink won't be able to restore
     * the previous Flink sink operator state (more specifically the committer operator state). You need to use {@code
     * --allowNonRestoredState} to ignore the previous sink state. During restore Flink sink state is used to check if
     * last commit was actually successful or not. {@code --allowNonRestoredState} can lead to data loss if the
     * Iceberg commit failed in the last completed checkpoint.
     *
     * @param newPrefix prefix for Flink sink operator uid and name
     * @return {@link Builder} to connect the iceberg table.
     */
    public Builder uidPrefix(String newPrefix) {
      this.uidPrefix = newPrefix;
      return this;
    }

    private <T> DataStreamSink<T> chainIcebergOperators() {
      Preconditions.checkArgument(inputCreator != null,
          "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
      Preconditions.checkNotNull(tableLoader, "Table loader shouldn't be null");

      DataStream<RowData> rowDataInput = inputCreator.apply(uidPrefix);

      if (table == null) {
        tableLoader.open();
        try (TableLoader loader = tableLoader) {
          this.table = loader.loadTable();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to load iceberg table from table loader: " + tableLoader, e);
        }
      }

      // Convert the requested flink table schema to flink row type.
      RowType flinkRowType = toFlinkRowType(table.schema(), tableSchema);

      // Distribute the records from input data stream based on the write.distribution-mode.
      DataStream<RowData> distributeStream = distributeDataStream(
          rowDataInput, table.properties(), table.spec(), table.schema(), flinkRowType);

      // Add parallel writers that append rows to files
      SingleOutputStreamOperator<WriteResult> writerStream = appendWriter(distributeStream);

      // Add single-parallelism committer that commits files
      // after successful checkpoint or end of input
      SingleOutputStreamOperator<Void> committerStream = appendCommitter(writerStream);

      // Add dummy discard sink
      return appendDummySink(committerStream);
    }

    /**
     * Append the iceberg sink operators to write records to iceberg table.
     *
     * @return {@link DataStreamSink} for sink.
     * @deprecated this will be removed in 0.14.0; use {@link #append()} because its returned {@link DataStreamSink}
     * has a more correct data type.
     */
    @Deprecated
    public DataStreamSink<RowData> build() {
      return chainIcebergOperators();
    }

    /**
     * Append the iceberg sink operators to write records to iceberg table.
     *
     * @return {@link DataStreamSink} for sink.
     */
    public DataStreamSink<Void> append() {
      return chainIcebergOperators();
    }

    private String operatorName(String suffix) {
      return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
    }

    @SuppressWarnings("unchecked")
    private <T> DataStreamSink<T> appendDummySink(SingleOutputStreamOperator<Void> committerStream) {
      DataStreamSink<T> resultStream = committerStream
          .addSink(new DiscardingSink())
          .name(operatorName(String.format("IcebergSink %s", this.table.name())))
          .setParallelism(1);
      if (uidPrefix != null) {
        resultStream = resultStream.uid(uidPrefix + "-dummysink");
      }
      return resultStream;
    }

    private SingleOutputStreamOperator<Void> appendCommitter(SingleOutputStreamOperator<WriteResult> writerStream) {
      IcebergFilesCommitter filesCommitter = new IcebergFilesCommitter(tableLoader, overwrite);
      SingleOutputStreamOperator<Void> committerStream = writerStream
          .transform(operatorName(ICEBERG_FILES_COMMITTER_NAME), Types.VOID, filesCommitter)
          .setParallelism(1)
          .setMaxParallelism(1);
      if (uidPrefix != null) {
        committerStream = committerStream.uid(uidPrefix + "-committer");
      }
      return committerStream;
    }

    private SingleOutputStreamOperator<WriteResult> appendWriter(DataStream<RowData> input) {
      // Find out the equality field id list based on the user-provided equality field column names.
      List<Integer> equalityFieldIds = Lists.newArrayList();
      if (equalityFieldColumns != null && equalityFieldColumns.size() > 0) {
        for (String column : equalityFieldColumns) {
          org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
          Preconditions.checkNotNull(field, "Missing required equality field column '%s' in table schema %s",
              column, table.schema());
          equalityFieldIds.add(field.fieldId());
        }
      }

      // Fallback to use upsert mode parsed from table properties if don't specify in job level.
      boolean upsertMode = upsert || PropertyUtil.propertyAsBoolean(table.properties(),
          UPSERT_ENABLED, UPSERT_ENABLED_DEFAULT);

      // Validate the equality fields and partition fields if we enable the upsert mode.
      if (upsertMode) {
        Preconditions.checkState(!overwrite,
            "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");
        Preconditions.checkState(!equalityFieldIds.isEmpty(),
            "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
        if (!table.spec().isUnpartitioned()) {
          for (PartitionField partitionField : table.spec().fields()) {
            Preconditions.checkState(equalityFieldIds.contains(partitionField.sourceId()),
                "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
                partitionField, equalityFieldColumns);
          }
        }
      }

      IcebergStreamWriter<RowData> streamWriter = createStreamWriter(tableLoader, equalityFieldIds, upsertMode);

      int parallelism = writeParallelism == null ? input.getParallelism() : writeParallelism;
      SingleOutputStreamOperator<WriteResult> writerStream = input
          .transform(operatorName(ICEBERG_STREAM_WRITER_NAME), TypeInformation.of(WriteResult.class), streamWriter)
          .setParallelism(parallelism);
      if (uidPrefix != null) {
        writerStream = writerStream.uid(uidPrefix + "-writer");
      }
      return writerStream;
    }

    private DataStream<RowData> distributeDataStream(DataStream<RowData> input,
                                                     Map<String, String> properties,
                                                     PartitionSpec partitionSpec,
                                                     Schema iSchema,
                                                     RowType flinkRowType) {
      DistributionMode writeMode;
      if (distributionMode == null) {
        // Fallback to use distribution mode parsed from table properties if don't specify in job level.
        String modeName = PropertyUtil.propertyAsString(properties,
            WRITE_DISTRIBUTION_MODE,
            WRITE_DISTRIBUTION_MODE_NONE);

        writeMode = DistributionMode.fromName(modeName);
      } else {
        writeMode = distributionMode;
      }

      switch (writeMode) {
        case NONE:
          return input;

        case HASH:
          if (partitionSpec.isUnpartitioned()) {
            return input;
          } else {
            return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
          }

        case RANGE:
          LOG.warn("Fallback to use 'none' distribution mode, because {}={} is not supported in flink now",
              WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName());
          return input;

        default:
          throw new RuntimeException("Unrecognized write.distribution-mode: " + writeMode);
      }
    }
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

  static IcebergStreamWriter<RowData> createStreamWriter(TableLoader tableLoader,
                                                         List<Integer> equalityFieldIds,
                                                         boolean upsert) {
    Preconditions.checkArgument(tableLoader != null, "Iceberg tableLoader should't be null");

    return new IcebergStreamWriter<>(tableLoader, equalityFieldIds,upsert);
  }
}
