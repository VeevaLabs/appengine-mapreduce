#!/usr/bin/env python
# Copyright 2010 Google Inc. All Rights Reserved.
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

"""Cloudstorage stub."""
import os
import re
import logging
import types
import itertools

from cloudstorage import storage_api
from cloudstorage import errors
from cloudstorage import common

# pylint: disable=too-many-locals, too-many-branches, too-many-statements
def compose(list_of_files, destination_file, files_metadata=None,
            content_type=None, retry_params=None, _account_id=None):
  """Runs the GCS Compose on the given files.
  Merges between 2 and 32 files into one file. Composite files may even
  be built from other existing composites, provided that the total
  component count does not exceed 1024. See here for details:
  https://cloud.google.com/storage/docs/composite-objects
  Args:
    list_of_files: List of file name strings with no leading slashes or bucket.
    destination_file: Path to the output file. Must have the bucket in the path.
    files_metadata: Optional, file metadata, order must match list_of_files,
      see link for available options:
      https://cloud.google.com/storage/docs/composite-objects#_Xml
    content_type: Optional, used to specify content-header of the output file.
    retry_params: Optional, an api_utils.RetryParams for this call to GCS.
      If None,the default one is used.
    _account_id: Internal-use only.
  Raises:
    ValueError: If the number of files is outside the range of 2-32.
  """
  
  logging.warn('Warning the cloudstorage libbrary is out of date and does'
               ' not have the needed library. Currently running an out of '
               'date stub. Please update your cloudstorage library.')
  api = storage_api._get_storage_api(retry_params=retry_params,
                                     account_id=_account_id)

  # Needed until cloudstorage_stub.py is updated to accept compose requests
  # TODO(rbruyere@gmail.com): When patched remove the True flow from this if.

  if os.getenv('SERVER_SOFTWARE').startswith('Dev'):
    def _temp_func(file_list, destination_file, content_type):
      """Dev server stub remove when the dev server accepts compose requests."""
      bucket = '/' + destination_file.split('/')[1] + '/'
      with open(destination_file, 'w', content_type=content_type) as gcs_merge:
        for source_file in file_list:
          with open(bucket + source_file['Name'], 'r') as gcs_source:
            gcs_merge.write(gcs_source.read())

    compose_object = _temp_func
  else:
    compose_object = api.compose_object
  file_list, _ = _validate_compose_list(destination_file,
                                        list_of_files,
                                        files_metadata, 32)
  compose_object(file_list, destination_file, content_type)


def _file_exists(destination):
  """Checks if a file exists.
  Tries to open the file.
  If it succeeds returns True otherwise False.
  Args:
    destination: Full path to the file (ie. /bucket/object) with leading slash.
  Returns:
    True if the file is accessible otherwise False.
  """
  try:
    with open(destination, "r"):
      return True
  except errors.NotFoundError:
    return False


def _validate_compose_list(destination_file, file_list,
                           files_metadata=None, number_of_files=32):
  """Validates the file_list and merges the file_list, files_metadata.
  Args:
    destination: Path to the file (ie. /destination_bucket/destination_file).
    file_list: List of files to compose, see compose for details.
    files_metadata: Meta details for each file in the file_list.
    number_of_files: Maximum number of files allowed in the list.
  Returns:
    A tuple (list_of_files, bucket):
      list_of_files: Ready to use dict version of the list.
      bucket: bucket name extracted from the file paths.
  """
  common.validate_file_path(destination_file)
  bucket = destination_file[0:(destination_file.index('/', 1) + 1)]
  try:
    if isinstance(file_list, types.StringTypes):
      raise TypeError
    list_len = len(file_list)
  except TypeError:
    raise TypeError('file_list must be a list')

  if list_len > number_of_files:
    raise ValueError(
          'Compose attempted to create composite with too many'
           '(%i) components; limit is (%i).' % (list_len, number_of_files))
  if list_len <= 1:
    raise ValueError('Compose operation requires at'
                     ' least two components; %i provided.' % list_len)

  if files_metadata is None:
    files_metadata = []
  elif len(files_metadata) > list_len:
    raise ValueError('files_metadata contains more entries(%i)'
                     ' than file_list(%i)'
                     % (len(files_metadata), list_len))
  list_of_files = []
  for source_file, meta_data in itertools.izip_longest(file_list,
                                                       files_metadata):
    if not isinstance(source_file, basestring):
      raise TypeError('Each item of file_list must be a string')
    if source_file.startswith('/'):
      logging.warn('Detected a "/" at the start of the file, '
                   'Unless the file name contains a "/" it '
                   ' may cause files to be misread')
    if source_file.startswith(bucket):
      logging.warn('Detected bucket name at the start of the file, '
                   'must not specify the bucket when listing file_names.'
                   ' May cause files to be misread')
    common.validate_file_path(bucket + source_file)

    list_entry = {}

    if meta_data is not None:
      list_entry.update(meta_data)
    list_entry["Name"] = source_file
    list_of_files.append(list_entry)

  return list_of_files, bucket
