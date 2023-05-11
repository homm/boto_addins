import os
import weakref
import logging
import xml.sax
import socket
from uuid import uuid4
import asyncio
from urllib.parse import quote

from boto.handler import XmlHandler
from boto.s3.acl import Policy
from boto.s3.connection import S3Connection, Bucket, Key
from tornado import httpclient, simple_httpclient, curl_httpclient


logger = logging.getLogger(__name__)


DATA_SIZE = 1024 * 1024
S3_TEMP_URL_TTL = 300  # seconds


class AsyncS3Connection(S3Connection):
    def __init__(self, *args, **kwargs):
        super(AsyncS3Connection, self).__init__(
            bucket_class=AsyncBucket, *args, **kwargs)

    def get_bucket(self, bucket_name, validate=False, headers=None):
        # Do not validate by default.
        return super(AsyncS3Connection, self).get_bucket(
            bucket_name, validate, headers)

    def generate_request(self, method, bucket='', key='',
                         query_args=None, headers=None,
                         content_length=None, attempts=1, **kwargs):
        if isinstance(bucket, self.bucket_class):
            bucket = bucket.name
        if isinstance(key, Key):
            key = key.name
        path = self.calling_format.build_path_base(bucket, key)
        auth_path = self.calling_format.build_auth_path(bucket, key)
        host = self.calling_format.build_host(self.server_name(), bucket)
        if query_args:
            path += '?' + query_args
            auth_path += '?' + query_args
        req = self.build_base_http_request(
            method, path, auth_path, headers=headers, host=host,
        )
        req.authorize(self)

        if content_length is None:
            del req.headers['Content-Length']
        else:
            req.headers['Content-Length'] = str(content_length)

        request = httpclient.HTTPRequest(
            "{0.protocol}://{0.host}{0.path}".format(req),
            req.method, req.headers, **kwargs
        )
        return fetch_request(request, attempts=attempts)


class AsyncBucket(Bucket):
    def __init__(self, connection=None, name=None):
        self._downloading_files = weakref.WeakValueDictionary()
        super(AsyncBucket, self).__init__(connection, name, key_class=AsyncKey)

    async def get_key_contents(self, key_name, attempts=3, **kwargs):
        return await self.connection.generate_request(
            'GET', self.name, key_name, attempts=attempts, **kwargs
        )

    async def get_key(self, key_name, headers=None, version_id=None,
                      response_headers=None, attempts=3):
        query_args_l = []
        if version_id:
            query_args_l.append('versionId=%s' % version_id)
        if response_headers:
            for rk, rv in response_headers.items():
                query_args_l.append('%s=%s' % (rk, quote(rv)))

        query_args = '&'.join(query_args_l) or None
        try:
            response = await self.connection.generate_request(
                'HEAD', self.name, key_name,
                headers=headers, query_args=query_args, attempts=attempts,
            )
        except httpclient.HTTPError as e:
            if e.code == 404:
                return False
            raise
        key = self.key_class(self)
        key.name = key_name
        clen = response.headers['content-length']
        key.size = int(clen) if clen else 0
        return key

    async def get_acl(self, key_name='', headers=None, version_id=None,
                      attempts=3):
        query_args = 'acl'
        if version_id:
            query_args += '&versionId=%s' % version_id

        resp = await self.connection.generate_request(
            'GET', self.name, key_name, query_args=query_args, headers=headers,
            attempts=attempts,
        )

        policy = Policy(self)
        h = XmlHandler(policy, self)
        xml.sax.parseString(resp.body, h)
        return policy

    async def download_key(self, key, path,
                           request_timeout=10 * 60, attempts=3):
        # Get shared downloading.
        future = self._downloading_files.get(key.name)

        if future is None:
            url = key.generate_url(S3_TEMP_URL_TTL)
            future = asyncio.ensure_future(http_download(
                url, path, request_timeout=request_timeout, attempts=attempts,
            ))

            # Share downloading.
            self._downloading_files[key.name] = future

        # Run task.
        await future


class AsyncKey(Key):
    async def exist(self):
        return await self.bucket.get_key(self.name)


async def fetch_request(request, client=None, retry_callback=None, attempts=1):
    if client is None:
        client = simple_httpclient.SimpleAsyncHTTPClient()

    if attempts <= 0:
        raise ValueError('attempts should be > 0')

    # Fail faster on connection if we can retry
    request.connect_timeout = 5 if attempts > 1 else 20

    # Save exceptions history for further analysis
    except_history = []

    while attempts:
        wait_before_retry = 0
        try:
            resp = await client.fetch(request)
        except httpclient.HTTPError as e:
            # retry on s3 errors
            if e.code == 500:
                wait_before_retry = 0.2
            elif e.code in (503, 599):
                wait_before_retry = 1
            else:
                raise
            except_history.append(e)
        except socket.error as e:
            # retry
            except_history.append(e)

        else:
            return resp

        attempts -= 1
        if not attempts:
            raise except_history[-1]

        if wait_before_retry:
            await asyncio.sleep(wait_before_retry)
        if retry_callback:
            await retry_callback(request, attempts)


async def http_download(source, destination,
                        request_timeout=10 * 60, attempts=3):
    tmp_name = '{}.{}'.format(destination, str(uuid4())[:8])

    try:
        with open(tmp_name, 'wb') as iobuffer:

            async def reset_iobuffer(*args):
                iobuffer.seek(0)
                iobuffer.truncate()

            await fetch_request(
                httpclient.HTTPRequest(
                    source,
                    # Direct write to file.
                    streaming_callback=iobuffer.write,
                    # Timeout for whole request, not tcp.
                    request_timeout=request_timeout,
                ),
                client=curl_httpclient.CurlAsyncHTTPClient(),
                retry_callback=reset_iobuffer,
                attempts=attempts,
            )

    except BaseException:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise

    # concurrent downloading
    if os.path.exists(destination):
        os.unlink(tmp_name)
    else:
        os.rename(tmp_name, destination)
