# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import google.api_core.exceptions
from google.cloud.bigquery_connection_v1.services import connection_service
import pytest
import test_utils.prefixer

from . import create_mysql_connection

connection_prefixer = test_utils.prefixer.Prefixer("py-bq-r", "snippets", separator="-")


@pytest.fixture(scope="session")
def location_path(
    connection_client: connection_service.ConnectionServiceClient(),
    project_id: str,
    location: str,
) -> str:
    return connection_client.common_location_path(project_id, location)


@pytest.fixture(scope="module", autouse=True)
def cleanup_connection(
    connection_client: connection_service.ConnectionServiceClient, location_path: str
) -> None:
    for connection in connection_client.list_connections(parent=location_path):
        connection_id = connection.name.split("/")[-1]
        if connection_prefixer.should_cleanup(connection_id):
            connection_client.delete_connection(name=connection.name)


@pytest.fixture(scope="session")
def connection_id(
    connection_client: connection_service.ConnectionServiceClient,
    project_id: str,
    location: str,
) -> str:
    id_ = connection_prefixer.create_prefix()
    yield id_

    connection_name = connection_client.connection_path(project_id, location, id_)
    try:
        connection_client.delete_connection(name=connection_name)
    except google.api_core.exceptions.NotFound:
        pass


def test_create_mysql_connection(
    capsys: pytest.CaptureFixture,
    project_id: str,
    location: str,
    connection_id: str,
    database: str,
    instance: str,
    instance_location: str,
    username: str,
    password: str,
) -> None:
    create_mysql_connection.main(
        project_id,
        location,
        connection_id,
        database,
        instance,
        instance_location,
        username,
        password,
    )
    out, _ = capsys.readouterr()
    assert "Created connection successfully:" in out
