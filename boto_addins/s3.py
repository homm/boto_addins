import os
import weakref
import logging
import xml.sax
import urllib
from uuid import uuid4

from boto.handler import XmlHandler
from boto.s3.acl import Policy
from boto.s3.connection import S3Connection, Bucket, Key
from boto.utils import merge_meta
from tornado import gen, httpclient, simple_httpclient, curl_httpclient


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

    def fetch_request(self, request,
                      client=simple_httpclient.SimpleAsyncHTTPClient):
        """
        For test purposes mainly.
        """
        return client().fetch(request)

    def generate_async_request(self, method, bucket='', key='',
                               query_args=None, headers=None,
                               content_length=None, **kwargs):
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

        async_request = httpclient.HTTPRequest(
            "{0.protocol}://{0.host}{0.path}".format(req),
            req.method, req.headers, **kwargs
        )
        return self.fetch_request(async_request)

    def async_set_contents_from_file(self, bucket, key_name, fp, headers=None,
                                     policy=None, encrypt_key=False,
                                     metadata=None):
        headers = headers or {}
        if policy:
            headers[self.provider.acl_header] = policy
        if encrypt_key:
            headers[self.provider.server_side_encryption_header] = 'AES256'
        if metadata is not None:
            headers = merge_meta(headers, metadata, self.provider)

        return self.generate_async_request(
            'PUT', bucket, key_name, headers=headers, body=fp.read(),
        )

    @gen.coroutine
    def async_copy_key(self, src_key, dst_key, temp_dir, metadata=None,
                       policy=None, encrypt_key=False, headers=None):
        headers = headers or {}
        if policy:
            headers[self.provider.acl_header] = policy
        if encrypt_key:
            headers[self.provider.server_side_encryption_header] = 'AES256'
        if metadata is not None:
            headers = merge_meta(headers, metadata, self.provider)

        url = src_key.generate_url(S3_TEMP_URL_TTL)
        destination = os.path.join(temp_dir, '_' + str(uuid4()))
        yield async_http_download(url, destination)

        @gen.coroutine
        def producer(write):
            with open(destination, 'r') as f:
                while True:
                    data = f.read(DATA_SIZE)
                    if not data:
                        break
                    yield write(data)

        try:
            yield self.generate_async_request(
                'PUT', dst_key.bucket.name, dst_key.name,
                headers=headers, body_producer=producer,
                content_length=src_key.size,
                request_timeout=10 * 60,  # Timeout for whole request, not tcp.
            )
        finally:
            if os.path.exists(destination):
                os.unlink(destination)


class AsyncBucket(Bucket):
    def __init__(self, connection=None, name=None):
        self._downloading_files = weakref.WeakValueDictionary()
        super(AsyncBucket, self).__init__(connection, name, key_class=AsyncKey)

    def async_get_key_contents(self, key_name, **kwargs):
        return self.connection.generate_async_request(
            'GET', self.name, key_name, **kwargs
        )

    @gen.coroutine
    def async_get_key(self, key_name, headers=None, version_id=None,
                      response_headers=None):
        query_args_l = []
        if version_id:
            query_args_l.append('versionId=%s' % version_id)
        if response_headers:
            for rk, rv in response_headers.iteritems():
                query_args_l.append('%s=%s' % (rk, urllib.quote(rv)))

        query_args = '&'.join(query_args_l) or None
        try:
            response = yield self.connection.generate_async_request(
                'HEAD', self.name, key_name,
                headers=headers, query_args=query_args,
            )
        except httpclient.HTTPError as e:
            if e.code == 404:
                raise gen.Return(False)
            raise
        key = self.key_class(self)
        key.name = key_name
        clen = response.headers['content-length']
        key.size = int(clen) if clen else 0
        raise gen.Return(key)

    @gen.coroutine
    def async_get_acl(self, key_name='', headers=None, version_id=None):
        query_args = 'acl'
        if version_id:
            query_args += '&versionId=%s' % version_id

        resp = yield self.connection.generate_async_request(
            'GET', self.name, key_name, query_args=query_args, headers=headers
        )

        policy = Policy(self)
        h = XmlHandler(policy, self)
        xml.sax.parseString(resp.body, h)
        raise gen.Return(policy)

    @gen.coroutine
    def async_download_key(self, key, path):
        # Get shared downloading.
        future = self._downloading_files.get(key.name)

        if future is None:
            url = key.generate_url(S3_TEMP_URL_TTL)
            future = async_http_download(url, path)

            # Share downloading.
            self._downloading_files[key.name] = future

        # Run task.
        yield future


class AsyncKey(Key):
    def async_exist(self):
        return self.bucket.async_get_key(self.name)


@gen.coroutine
def async_http_download(source, destination):
    tmp_name = '{}.{}'.format(destination, str(uuid4())[:8])

    try:
        with open(tmp_name, 'w') as iobuffer:
            yield curl_httpclient.CurlAsyncHTTPClient().fetch(
                source,
                streaming_callback=iobuffer.write,  # Direct write to file.
                request_timeout=10 * 60,  # Timeout for whole request, not tcp.
            )

    except BaseException:
        os.unlink(tmp_name)
        raise

    # concurrent downloading
    if os.path.exists(destination):
        os.unlink(tmp_name)
    else:
        os.rename(tmp_name, destination)
