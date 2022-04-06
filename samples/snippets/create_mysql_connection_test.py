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

from multiprocessing.connection import _ConnectionBase

import google.api_core.exceptions
from google.cloud import bigquery_connection_v1 as bq_connection
from google.cloud.bigquery_connection_v1.services import connection_service
import pytest
from samples.snippets.conftest import (
    cloud_sql_conn_name, 
    database, 
    location, 
    mysql_password, 
    project_id, 
    username
)
import test_utils.prefixer

from . import create_mysql_connection

connection_prefixer = test_utils.prefixer.Prefixer("py-bq-r", "snippets", separator="-")

test_project_id = project_id
test_location = location
test_database = database
test_username = username
test_password = mysql_password
test_cloud_sql_conn_name = cloud_sql_conn_name


@pytest.fixture(scope="session")
def location_path(
    connection_client: connection_service.ConnectionServiceClient(),
    test_project_id: str,
    test_location: str,
) -> str:
    return connection_client.common_location_path(test_project_id, test_location)


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
    test_project_id: str,
    test_location: str,
) -> str:
    id_ = connection_prefixer.create_prefix()
    yield id_

    connection_name = connection_client.connection_path(
        test_project_id, test_location, id_
    )
    try:
        connection_client.delete_connection(name=connection_name)
    except google.api_core.exceptions.NotFound:
        pass


def test_create_mysql_connection(capsys: pytest.CaptureFixture) -> None:
    test_cloud_sql_credential = _ConnectionBase.CloudSqlCredential(
        {
            "username": test_username,
            "password": test_password,
        }
    )
    test_cloud_sql_properties = bq_connection.CloudSqlProperties(
        {
            "type_": bq_connection.CloudSqlProperties.DatabaseType.MYSQL,
            "database": test_database,
            "instance_id": test_cloud_sql_conn_name,
            "credential": test_cloud_sql_credential,
        }
    )
    create_mysql_connection.create_mysql_connection(
        project_id=test_project_id,
        location=test_location,
        cloud_sql_properties=test_cloud_sql_properties,
    )
    out, _ = capsys.readouterr()
    assert "Created connection successfully:" in out
