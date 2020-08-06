# Copyright 2020 Google LLC
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

"""This script is used to synthesize generated parts of this library."""
import os

import synthtool as s
import synthtool.gcp as gcp
from synthtool.languages import python

gapic = gcp.GAPICBazel()
common = gcp.CommonTemplates()

# ----------------------------------------------------------------------------
# Generate access approval GAPIC layer
# ----------------------------------------------------------------------------
library = gapic.py_library(
    service="bigquery/connection",
    version="v1",
    bazel_target=f"//google/cloud/bigquery/connection/v1:bigquery-connection-v1-py"
)

s.move(library, excludes=["setup.py", "README.rst", "docs/index.rst"])

# ----------------------------------------------------------------------------
# Add templated files
# ----------------------------------------------------------------------------
templated_files = common.py_library(
    cov_level=100,
    microgenerator=True,
)
s.move(
    templated_files, excludes=[".coveragerc"]
)  # the microgenerator has a good coveragerc file


# Expand flake errors permitted to accomodate the Microgenerator
# TODO: remove extra error codes once issues below are resolved
# F401: https://github.com/googleapis/gapic-generator-python/issues/324
# F841: local variable 'client'/'response' is assigned to but never use
s.replace(".flake8", "ignore = .*", "ignore = E203, E266, E501, W503, F401, F841")


s.shell.run(["nox", "-s", "blacken"], hide_output=False)
