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

# [START bigqueryconnection_connection_create]
from google.cloud import bigquery_connection_v1 as bq_connection
from google.cloud.bigquery_connection_v1 import types as connection_types

"""This sample shows how to create a BigQuery connection with a Cloud MySql database"""


def main(
    project_id: str,
    location: str,
    connection_id: str,
    database: str,
    instance: str,
    instance_location: str,
    username: str,
    password: str,
) -> connection_types.Connection:
    # [START_EXCLUDE]
    original_project_id = project_id
    original_location = location
    original_connection_id = connection_id
    original_database = database
    original_instance = instance
    original_instance_location = instance_location
    original_username = username
    original_password = password
    # [END_EXCLUDE]
    # TODO(developer): Set project_id to the project ID containing the
    # connection.
    project_id = "your-project-id"
    # TODO(developer): Set location to the location of the connection.
    # See: https://cloud.google.com/bigquery/docs/locations for a list of
    # available locations.
    location = "US"
    # TODO(developer): Set connection_id to the connection ID.
    connection_id = "your-connection-id"
    # TODO(developer): Set database to the name of the database in which you're creating the connection.
    database = "my-database"
    # TODO(developer): Set instance to the instance where you're creating the connection.
    instance = "my-instance"
    # TODO(developer): Set instance_location to the location of the instance where you are creating the connection.
    instance_location = "my-instance-location"
    # TODO(developer): Set username to the database username.
    username = "my-username"
    # TODO(developer): Set password to the database password.
    password = "my-password"
    instance_id = f"{project_id}:{instance_location}:{instance}"

    # [START_EXCLUDE]
    project_id = original_project_id
    location = original_location
    connection_id = original_connection_id
    database = original_database
    instance = original_instance
    instance_location = original_instance_location
    username = original_username
    password = original_password
    instance_id = f"{project_id}:{instance_location}:{instance}"
    # [END_EXCLUDE]
    cloud_sql_credential = bq_connection.CloudSqlCredential()
    cloud_sql_credential.username = username
    cloud_sql_credential.password = password
    cloud_sql_properties = bq_connection.CloudSqlProperties()
    cloud_sql_properties.type_ = bq_connection.CloudSqlProperties.DatabaseType.MYSQL
    cloud_sql_properties.database = database
    cloud_sql_properties.instance_id = instance_id
    cloud_sql_properties.credential = cloud_sql_credential
    connection = connection_types.Connection()
    connection.cloud_sql = cloud_sql_properties
    create_connection(project_id, location, connection_id, connection)
    return connection


def create_connection(
    project_id: str,
    location: str,
    connection_id: str,
    connection: bq_connection.Connection,
) -> None:
    client = bq_connection.ConnectionServiceClient()
    parent = client.common_location_path(project_id, location)
    request = bq_connection.CreateConnectionRequest()
    request.parent = parent
    request.connection = connection
    request.connection_id = connection_id
    response = client.create_connection(request)
    print(f"Created connection successfully: {response.name}")


# [END bigqueryconnection_connection_create]
