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
from google.cloud.bigquery_connection_v1.types import connection as connection_types

def main(project_id: str "your-project-id", location: str = "US") -> None:
    client = bq_connection.ConnectionServiceClient()
    create_connection(project_id, location, client)

def create_connection(project_id: str, location: str, client: bq_connection.ConnectionServiceClient) -> connection_types.Connection:
    original_project_id = project_id
    original_location = location
    # [START bigqueryconnection_connection_create]
    # TODO(developer): Set project_id to the project ID containing the 
    # connection.
    project_id = "your-project-id"

    # TODO(developer): Set location to the location of the connection.
    # See: https://cloud.google.com/bigquery/docs/locations for a list of
    # available locations.
    location = "US"

    # TODO(developer): CloudSQL connection info

    parent = client.common_location_path(project_id, location)
    connection = client.create_connection(parent, project_id, location)
    print(f"Created connection: {connection.name}")
    return connection


# [END bigqueryconnection_connection_create]