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

## Concurrent writers on S3

Every independent writer must open the table with the same OCC and lock
configuration. The storage lock uses S3 conditional puts against
`.hoodie/.locks/table_lock.json`; credentials and region are supplied through
the normal `object_store` options, environment variables, or the AWS workload
identity/instance role.

```python
import os

import polars as pl

from hudi.polars import HudiPolarsTable

OCC_OPTIONS = {
    "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
    "hoodie.write.lock.provider": (
        "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider"
    ),
    "hoodie.cleaner.policy.failed.writes": "LAZY",
}

table = HudiPolarsTable.open(
    "s3://my-bucket/lake/trips",
    hudi_options=OCC_OPTIONS,
    storage_options={"aws_region": os.environ["AWS_REGION"]},
)

table.upsert(
    pl.DataFrame(
        {
            "id": ["a"],
            "city": ["santiago"],
            "ts": [3],
            "fare": [12.0],
        }
    )
)
```

For a local S3-compatible endpoint such as RustFS or MinIO, add the endpoint
and path-style HTTP settings. Credentials can remain in `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY`:

```python
storage_options = {
    "aws_endpoint_url": "http://127.0.0.1:9000",
    "aws_allow_http": "true",
    "aws_region": "us-east-1",
    "aws_virtual_hosted_style_request": "false",
}
```

Use unique Hudi record keys across independent append-only producers when the
application requires key uniqueness: the default Apache Hudi conflict strategy
detects file-group overlap, not two inserts of the same record key into two new
file groups. See [Concurrent Writers and Optimistic Concurrency
Control](concurrent-writers.md) for the protocol and tuning options.

The adapter materializes a `LazyFrame` before writing. Reads return a
`DataFrame`; `read_stream()` yields one `DataFrame` per Arrow batch. It is not
yet a native Polars lazy scan source, so Polars expressions are not pushed into
Hudi automatically. Use `HudiReadOptions` for Hudi filter and projection
pushdown.

## Current constraints

- Each writer is single-node, but independent writers can use operation-aware
  OCC with the storage-based lock on S3, GCS, or Azure. Deletes and ordinary
  updates conflict by file group, dynamic overwrite by target partition, and
  full overwrite across the table. Local filesystems do not provide the
  conditional-update primitive required by this lock provider.
- Record keys must currently be a single Arrow `string` field. Complex keys,
  non-string keys, and additional key generators are not implemented.
- Write-side schema evolution rejects changes beyond nullability alignment.
- CDC write/read surfaces and non-Parquet write formats are not implemented.
- The core writer is based on upstream PR
  [apache/hudi-rs#666](https://github.com/apache/hudi-rs/pull/666). Until that
  work and the Python bindings merge upstream, this API must be built from the
  feature branch rather than installed from the released PyPI package.
