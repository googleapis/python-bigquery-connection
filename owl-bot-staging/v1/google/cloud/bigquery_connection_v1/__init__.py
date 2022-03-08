# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from .services.connection_service import ConnectionServiceClient
from .services.connection_service import ConnectionServiceAsyncClient

from .types.connection import AwsAccessRole
from .types.connection import AwsCrossAccountRole
from .types.connection import AwsProperties
from .types.connection import CloudResourceProperties
from .types.connection import CloudSpannerProperties
from .types.connection import CloudSqlCredential
from .types.connection import CloudSqlProperties
from .types.connection import Connection
from .types.connection import CreateConnectionRequest
from .types.connection import DeleteConnectionRequest
from .types.connection import GetConnectionRequest
from .types.connection import ListConnectionsRequest
from .types.connection import ListConnectionsResponse
from .types.connection import UpdateConnectionRequest

__all__ = (
    'ConnectionServiceAsyncClient',
'AwsAccessRole',
'AwsCrossAccountRole',
'AwsProperties',
'CloudResourceProperties',
'CloudSpannerProperties',
'CloudSqlCredential',
'CloudSqlProperties',
'Connection',
'ConnectionServiceClient',
'CreateConnectionRequest',
'DeleteConnectionRequest',
'GetConnectionRequest',
'ListConnectionsRequest',
'ListConnectionsResponse',
'UpdateConnectionRequest',
)