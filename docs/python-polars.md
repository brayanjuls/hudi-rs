<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Python and Polars writes

The Python extension exposes the native Arrow writer through `HudiTable`.
`HudiPolarsTable` adds conversion between Polars and PyArrow at that boundary.
It supports table creation, append, upsert, overwrite, dynamic partition
overwrite, update, delete, snapshot/incremental reads, and streaming reads for
copy-on-write and merge-on-read tables.

```python
import polars as pl

from hudi.polars import HudiPolarsTable

table = HudiPolarsTable.create(
    "/tmp/trips",
    "trips",
    record_key_fields=["id"],
    partition_fields=["city"],
    ordering_fields=["ts"],
)

table.append(
    pl.DataFrame(
        {
            "id": ["a", "b"],
            "city": ["santiago", "valparaiso"],
            "ts": [1, 1],
            "fare": [10.0, 20.0],
        }
    )
)
table.upsert(
    pl.DataFrame(
        {
            "id": ["b", "c"],
            "city": ["santiago", "concepcion"],
            "ts": [2, 1],
            "fare": [25.0, 30.0],
        }
    ).lazy()
)

result = table.read()
```

The adapter materializes a `LazyFrame` before writing. Reads return a
`DataFrame`; `read_stream()` yields one `DataFrame` per Arrow batch. It is not
yet a native Polars lazy scan source, so Polars expressions are not pushed into
Hudi automatically. Use `HudiReadOptions` for Hudi filter and projection
pushdown.

## Current constraints

- The native writer is single-node and single-writer. External lock providers
  and optimistic concurrency control are not implemented.
- Record keys must currently be a single Arrow `string` field. Complex keys,
  non-string keys, and additional key generators are not implemented.
- Write-side schema evolution rejects changes beyond nullability alignment.
- CDC write/read surfaces and non-Parquet write formats are not implemented.
- The core writer is based on upstream PR
  [apache/hudi-rs#666](https://github.com/apache/hudi-rs/pull/666). Until that
  work and the Python bindings merge upstream, this API must be built from the
  feature branch rather than installed from the released PyPI package.
