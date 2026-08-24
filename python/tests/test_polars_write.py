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

import polars as pl
import pytest

from hudi.polars import HudiPolarsTable


@pytest.mark.parametrize("table_type", ["COPY_ON_WRITE", "MERGE_ON_READ"])
def test_polars_append_upsert_read_and_reopen(tmp_path, table_type):
    base_uri = str(tmp_path / table_type.lower())
    table = HudiPolarsTable.create(
        base_uri,
        "trips",
        table_type=table_type,
        record_key_fields=["id"],
        partition_fields=["city"],
        ordering_fields=["ts"],
    )

    append_result = table.append(
        pl.DataFrame(
            {
                "id": ["a", "b"],
                "city": ["santiago", "valparaiso"],
                "ts": [1, 1],
                "fare": [10.0, 20.0],
            }
        )
    )
    assert append_result.num_rows == 2
    assert append_result.instant

    upsert_result = table.write(
        pl.LazyFrame(
            {
                "id": ["b", "c"],
                "city": ["santiago", "concepcion"],
                "ts": [2, 1],
                "fare": [25.0, 30.0],
            }
        ),
        mode="upsert",
    )
    assert upsert_result.num_updates == 1
    assert upsert_result.num_inserts == 1

    expected = [
        {"id": "a", "city": "santiago", "ts": 1, "fare": 10.0},
        {"id": "b", "city": "santiago", "ts": 2, "fare": 25.0},
        {"id": "c", "city": "concepcion", "ts": 1, "fare": 30.0},
    ]
    assert table.read().sort("id").to_dicts() == expected

    reopened = HudiPolarsTable.open(base_uri)
    assert reopened.read().sort("id").to_dicts() == expected


def test_polars_write_rejects_invalid_batch_size(tmp_path):
    table = HudiPolarsTable.create(
        str(tmp_path / "trips"),
        "trips",
        record_key_fields=["id"],
        metadata_enabled=False,
    )

    with pytest.raises(ValueError, match="batch_size must be greater than zero"):
        table.append(pl.DataFrame({"id": ["a"]}), batch_size=0)
