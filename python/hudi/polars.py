# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Polars-friendly Hudi table reads and writes."""

from collections.abc import Iterator
from typing import Literal, Optional, Union

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]

from hudi._internal import (
    HudiAppendResult,
    HudiReadOptions,
    HudiTable,
    HudiWriteResult,
    build_hudi_table,
)

Frame = Union[pl.DataFrame, pl.LazyFrame]
WriteMode = Literal[
    "append",
    "append_only",
    "upsert",
    "overwrite",
    "dynamic_partition_overwrite",
]


def _materialize(data: Frame) -> pl.DataFrame:
    return data.collect() if isinstance(data, pl.LazyFrame) else data


def _to_record_batches(
    data: Frame, batch_size: Optional[int] = None
) -> list[pa.RecordBatch]:
    if batch_size is not None and batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    arrow_table = _normalize_arrow_types(_materialize(data).to_arrow())
    if batch_size is None:
        return list(arrow_table.to_batches())
    return list(arrow_table.to_batches(max_chunksize=batch_size))


def _normalize_arrow_types(table: pa.Table) -> pa.Table:
    """Use Arrow types accepted by the native writer for Polars string columns."""
    fields = []
    changed = False
    for field in table.schema:
        if pa.types.is_large_string(field.type) or pa.types.is_string_view(field.type):
            fields.append(
                pa.field(field.name, pa.string(), field.nullable, field.metadata)
            )
            changed = True
        else:
            fields.append(field)
    if not changed:
        return table
    return table.cast(pa.schema(fields, metadata=table.schema.metadata))


def _from_arrow(data: object, include_meta_fields: bool) -> pl.DataFrame:
    frame = pl.from_arrow(data, rechunk=False)
    if not isinstance(frame, pl.DataFrame):
        raise TypeError("expected Arrow tabular data to produce a Polars DataFrame")
    if include_meta_fields:
        return frame
    meta_columns = [name for name in frame.columns if name.startswith("_hoodie_")]
    return frame.drop(meta_columns)


class HudiPolarsTable:
    """A Polars-facing wrapper around the native :class:`hudi.HudiTable`."""

    def __init__(self, table: HudiTable) -> None:
        self._table = table

    @classmethod
    def create(
        cls,
        base_uri: str,
        table_name: str,
        *,
        table_type: str = "COPY_ON_WRITE",
        record_key_fields: Optional[list[str]] = None,
        partition_fields: Optional[list[str]] = None,
        ordering_fields: Optional[list[str]] = None,
        table_version: int = 9,
        metadata_enabled: bool = True,
        record_index_enabled: Optional[bool] = None,
        column_stats_enabled: Optional[bool] = None,
        partition_stats_enabled: Optional[bool] = None,
        hive_style_partitioning: bool = True,
        hudi_options: Optional[dict[str, str]] = None,
        storage_options: Optional[dict[str, str]] = None,
    ) -> "HudiPolarsTable":
        """Create a new native Hudi table and open it for Polars operations."""
        table = HudiTable.create(
            base_uri,
            table_name,
            table_type,
            record_key_fields,
            partition_fields,
            ordering_fields,
            table_version,
            metadata_enabled,
            record_index_enabled,
            column_stats_enabled,
            partition_stats_enabled,
            hive_style_partitioning,
            hudi_options,
            storage_options,
        )
        return cls(table)

    @classmethod
    def open(
        cls,
        base_uri: str,
        *,
        hudi_options: Optional[dict[str, str]] = None,
        storage_options: Optional[dict[str, str]] = None,
    ) -> "HudiPolarsTable":
        """Open an existing Hudi table."""
        return cls(
            build_hudi_table(
                base_uri,
                hudi_options=hudi_options,
                storage_options=storage_options,
            )
        )

    @property
    def native_table(self) -> HudiTable:
        """Return the underlying native table handle."""
        return self._table

    def read(
        self,
        options: Optional[HudiReadOptions] = None,
        *,
        include_meta_fields: bool = False,
    ) -> pl.DataFrame:
        """Read a Hudi snapshot or incremental query into a Polars DataFrame."""
        batches = self._table.read(options)
        return _from_arrow(pa.Table.from_batches(batches), include_meta_fields)

    def read_stream(
        self,
        options: Optional[HudiReadOptions] = None,
        *,
        include_meta_fields: bool = False,
    ) -> Iterator[pl.DataFrame]:
        """Yield Polars DataFrames from the native record-batch stream."""
        for batch in self._table.read_stream(options):
            yield _from_arrow(batch, include_meta_fields)

    def append(
        self, data: Frame, *, batch_size: Optional[int] = None
    ) -> HudiAppendResult:
        """Append rows without looking up existing record keys."""
        return self._table.append(_to_record_batches(data, batch_size))

    def append_only(
        self, data: Frame, *, batch_size: Optional[int] = None
    ) -> HudiAppendResult:
        """Append rows to a table configured with strict append-only merging."""
        return self._table.append_only(_to_record_batches(data, batch_size))

    def upsert(
        self,
        data: Frame,
        *,
        update_columns: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
    ) -> HudiWriteResult:
        """Insert new keys and replace existing keys using Hudi merge semantics."""
        return self._table.upsert(
            _to_record_batches(data, batch_size), update_columns=update_columns
        )

    def overwrite(
        self, data: Frame, *, batch_size: Optional[int] = None
    ) -> HudiWriteResult:
        """Replace all rows in the table."""
        return self._table.overwrite(_to_record_batches(data, batch_size))

    def dynamic_partition_overwrite(
        self, data: Frame, *, batch_size: Optional[int] = None
    ) -> HudiWriteResult:
        """Replace only partitions represented by the input rows."""
        return self._table.dynamic_partition_overwrite(
            _to_record_batches(data, batch_size)
        )

    def write(
        self,
        data: Frame,
        *,
        mode: WriteMode = "append",
        update_columns: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
    ) -> Union[HudiAppendResult, HudiWriteResult]:
        """Write a Polars frame using an explicit Hudi operation mode."""
        if mode == "append":
            return self.append(data, batch_size=batch_size)
        if mode == "append_only":
            return self.append_only(data, batch_size=batch_size)
        if mode == "upsert":
            return self.upsert(
                data, update_columns=update_columns, batch_size=batch_size
            )
        if mode == "overwrite":
            return self.overwrite(data, batch_size=batch_size)
        if mode == "dynamic_partition_overwrite":
            return self.dynamic_partition_overwrite(data, batch_size=batch_size)
        raise ValueError(f"unsupported Hudi write mode: {mode}")

    def delete(self, filter: str) -> HudiWriteResult:
        """Delete rows matching a Hudi write-filter expression."""
        return self._table.delete(filter)

    def update(self, filter: str, values: Frame) -> HudiWriteResult:
        """Update matching rows from a one-row Polars frame."""
        batches = _to_record_batches(values)
        if len(batches) != 1:
            raise ValueError(
                "update values must produce exactly one Arrow record batch"
            )
        return self._table.update(filter, batches[0])


__all__ = ["Frame", "HudiPolarsTable", "WriteMode"]
