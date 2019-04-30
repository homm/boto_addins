from __future__ import unicode_literals

from tornado_botocore import Botocore
import botocore.session
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado import gen


def _make_method(name):
    def method(self, *args, **kwargs):
        return gen.Task(self._get_method(name),
                        QueueUrl=self.queue_url, *args, **kwargs)
    return method


class SQS(object):
    def __init__(self, queue, account_id, region, access_key, secret_key):
        self.queue = queue
        self.account_id = account_id
        self.region = region
        self.session = botocore.session.Session()
        self.session.set_credentials(access_key, secret_key)
        self.http_client = CurlAsyncHTTPClient()
        self._methods = {}

    @property
    def queue_url(self):
        return 'https://sqs.{}.amazonaws.com/{}/{}'.format(
            self.region, self.account_id, self.queue,
        )

    def _get_method(self, name):
        method = self._methods.get(name)
        if method is None:
            method = Botocore('sqs', name,
                              region_name=self.region, session=self.session)
            method.http_client = self.http_client
            self._methods[name] = method
        return method.call

    (send_message, receive_message, purge_queue,
     change_message_visibility, delete_message) = map(_make_method, [
        'SendMessage', 'ReceiveMessage', 'PurgeQueue',
        'ChangeMessageVisibility', 'DeleteMessage',
    ])
