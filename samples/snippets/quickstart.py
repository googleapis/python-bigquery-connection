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

# [START bigqueryconnection_quickstart]
import argparse

from google.cloud import bigquery_connection_v1 as bq_connection


def main(project_id: str = "your-project-id", location: str = "US") -> None:
    # Constructs client for interacting with the service
    client = bq_connection.ConnectionServiceClient()
    list_connections(client, project_id, location)


def list_connections(
    client: bq_connection.ConnectionServiceClient,
    project_id: str = "your-project-id",
    location: str = "US",
) -> None:
    """Prints details and summary information about connections for a given admin project and location"""
    print(f"List of connections in project {project_id} in location {location}")
    req = bq_connection.ListConnectionsRequest(
        parent=client.common_location_path(project_id, location)
    )
    for connection in client.list_connections(request=req):
        print(f"\tConnection {connection.friendly_name} ({connection.name})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", type=str)
    parser.add_argument("--location", default="US", type=str)
    args = parser.parse_args()
    main(project_id=args.project_id, location=args.location)

# [END bigqueryreservation_quickstart]
