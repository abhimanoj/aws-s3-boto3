# encoding: utf-8

"""
This is built as a wrapper on top of boto3 adding
support to use s3 URI format.

* Also supports passing file URL during local development testing
* **Example:** This wrapper supports both below two for the URI

  * s3://bucket/file/key.json
  * file:///path/to/file.json

This library exposes three functions

* **get_data_bytes**

  * As the name implies, it is used to retrieve the content from s3 as bytes
    instead of considering as string.

* **get_data**

  * gets the data from s3 and returns it as python string

* **put_data**

  * Uploads the provided data to s3.

Example Usage
-------------
::

   from library_name import get_data, put_data
   import json

   request_id = "10023041"
   # getting data by passing s3 path
   data = get_data(f"s3://bucket/folder/{request_id}.json")

   # example of writing data
   data = {"data": "sample content"}

   # the data needs to be explicitly dumped.
   put_data("s3://bucket/folder/{request_id}.json", data=json.dumps(data))

"""

__all__ = ["get_data_bytes", "get_data", "put_data"]

import os
import shutil
import time
from io import TextIOWrapper
from typing import TextIO, Union, BinaryIO
from urllib.parse import urlparse
from logging import getLogger

import boto3

logger = getLogger(__name__)

boto3 = boto3.Session(
    aws_access_key_id='<APP-ID>',
    aws_secret_access_key='<APP-KAY>',
    region_name='<region>', 
)
 


def put_data(
        url: str, data: Union[str, bytes, BinaryIO, TextIO]) -> None:
    """
    Uploads the given data to the specified URL. If data type
    is str, it will always encoded to utf-8 to form bytes.

    :param url: Uniform resource locator.
                **Example** ::

                    file:///path/to/file.txt or s3://path/to/file.txt

    :param data: data can be a string or bytes or file like object
    """

    parsed_url = urlparse(url)
    assert parsed_url.scheme in ("file", "s3"), \
        "Unsupported schema type ({schema_type}) in url({url})".format(
            schema_type=parsed_url.scheme, url=url
        )

    if parsed_url.scheme == "s3":
        s3 = boto3.resource('s3')
        s3_file = s3.Object(parsed_url.netloc, parsed_url.path.strip("/"))

        if isinstance(data, str):
            data = data.encode("utf-8")

        # Exception will be raised if there is an issue uploading file
        s3_file.put(Body=data)

        # https://github.com/boto/boto3/issues/1067#issuecomment-323798116
        # There is chance that file upload fails silently.
        # So, doing a check to make sure it is uploaded.

        # waiting a second before checking
        time.sleep(1)
        try:
            # only doing head object
            s3_file.load()
        except Exception as e:
            raise RuntimeError(
                "Failed to upload file (Error - %s: %s). "
                "Bucket: %s, path: %s" % (
                    type(e), str(e),
                    parsed_url.netloc, parsed_url.path.strip("/"))
            )
    else:
        os.makedirs(os.path.dirname(parsed_url.path), exist_ok=True)
        with open(parsed_url.path, "wb") as out:

            if isinstance(data, str):
                out.write(data.encode("utf-8"))
            elif isinstance(data, (bytes, bytearray)):
                out.write(data)
            else:
                try:
                    shutil.copyfileobj(data, out)
                except AttributeError:
                    raise ValueError(
                        "Data should of type Str, bytes or file"
                        " like object. But given"
                        " {type}".format(type=type(data)))


def get_data_file(url: str) -> BinaryIO:
    """
    Returns a file like object for a resource specified using the URL.
    Currently only URL with schema file:// and s3:// supported.

    :param url: Uniform resource locator.
                **Example** ::

                    file:///path/to/file.txt or s3://path/to/file.txt

    :return: file like object
    """

    parsed_url = urlparse(url)

    # print(parsed_url.scheme,'---')

    assert parsed_url.scheme in ("file", "s3"), \
        "Unsupported schema type ({schema_type}) in url({url})".format(
            schema_type=parsed_url.scheme, url=url
        )

    if parsed_url.scheme == "s3":
        s3 = boto3.client('s3')

        obj = s3.get_object(
            Bucket=parsed_url.netloc,
            Key=parsed_url.path.strip("/")
        )
        return obj['Body']
    else:
        return open(parsed_url.path, "rb")


def get_data_bytes(url: str) -> bytes:
    """
    Downloads the data and returns it as bytes object for a resource specified
    using the URL.

    Currently only URL with schema file:// and s3:// supported.

    :param url: Uniform resource locator.
                **Example** ::

                    file:///path/to/file.txt or s3://path/to/file.txt

    :return: the binary content of the requested resource.
    """
    f = get_data_file(url)
    data = f.read()

    try:
        f.close()
    except AttributeError:
        pass

    return data


def get_data(url: str, encoding="utf-8", stream=False) -> Union[str, TextIO]:
    """
    Downloads the data and returns it as python string or a stream object
    based on the param 'stream'.

    Currently only URL with schema file:// and s3:// supported.

    :param url: Uniform resource locator.
                Example: file:///path/to/file.txt or s3://path/to/file.txt
    :param encoding: text encoding value. Example: utf-8, ascii
    :param stream: if it is False, str object will be
                   returned else file like object
    :return: str or file like object
    """

    if stream is True:
        byte_stream = get_data_file(url)
        byte_stream.readable = lambda *_args, **_kwargs: True
        byte_stream.writable = lambda *_args, **_kwargs: False
        byte_stream.seekable = lambda *_args, **_kwargs: False
        # noinspection PyPropertyAccess
        byte_stream.closed = False
        return TextIOWrapper(byte_stream, encoding=encoding)
    else:
        return str(get_data_bytes(url), encoding=encoding)
