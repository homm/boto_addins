from __future__ import unicode_literals

import json

from six.moves.urllib_parse import quote

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from tornado import gen
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.httpclient import HTTPRequest
from yurl import URL


class LambdaCallError(Exception):
    """AWS Lambda remote call exception.

    :param error_type: Error type: `Handled` and `Unhandled`
    :param message: Error message.
    :param trace: Call stack trace from AWS Lambda call.

    """
    def __init__(self, error_type, message, trace):
        self.error_type = error_type
        self.message = message
        self.trace = trace
        super(LambdaCallError, self).__init__(str(self))

    def __str__(self):
        return "{0}: {1}".format(self.error_type, self.message)


class Lambda(object):
    region = None
    API_VERSION = '2015-03-31'

    def __init__(self, name, credentials, region, qualifier='$LATEST'):
        """Creates a client of AWS Lambda function with ability to invoke
         it synchronously by RequestResponse invocation type.

         By `synchronously` it means, that caller will receive lambda
         function call result in place. Request to AWS Lambda will be made
         asynchronously.

         See http://docs.aws.amazon.com/lambda/latest/dg/API_Invoke.html
         for details.

        Usage example:

          _ioloop = ioloop.IOLoop.instance()

          @gen.coroutine
          def async_request():
            credentials = Credentials(access_key=<access_key>,
                                      secret_key=<secret_key>)
            payload = {'input_bucket': 'bucket', ...}
            service = Lambda('some-service', credentials, <region>)
            result = yield service(payload)
            _ioloop.stop()

          _ioloop.add_callback(async_request)
          _ioloop.start()

        :param name: Name of the AWS Lambda function.
        :param credentials: AWS credentials.
        :param region: AWS Lambda function region.
        :param qualifier: Lambda function alias or version.

        """
        self.name = name
        self.region = region
        self._credentials = credentials
        self.client = CurlAsyncHTTPClient()

        if qualifier:
            query = 'Qualifier={0}'.format(quote(qualifier))
        else:
            query = None
        self.url = URL(scheme='https',
                       host='lambda.{0}.amazonaws.com'.format(region),
                       path='{0}/functions/{1}/invocations'.format(
                           self.API_VERSION, name),
                       query=query)
        self.url = str(self.url)

    def __sign_request(self, request):
        """Sign request to AWS with SigV4Auth."""
        aws_request = AWSRequest(method=request.method, url=request.url,
                                 data=request.body)
        signer = SigV4Auth(self._credentials, 'lambda', self.region)
        signer.add_auth(aws_request)
        request.headers.update(dict(aws_request.headers.items()))

    @gen.coroutine
    def __call__(self, payload):
        """Make async call to synchronously invoke AWS Lambda function with
        `payload` data.

        :param dict payload: Data payload for function call.

        :return function result in JSON.

        :raise LambdaCallError: if any error on AWS Lambda side.

        """
        headers = {'Content-Type': 'application/json'}
        request = HTTPRequest(
            method='POST', url=self.url, headers=headers,
            # Maximum execution duration per request is 300 seconds
            request_timeout=10 * 60,
            body=json.dumps(payload)
        )
        self.__sign_request(request)
        response = yield self.client.fetch(request)
        error = response.headers.get('X-Amz-Function-Error', None)
        body = json.loads(response.body)
        if error:
            if 'errorType' in body:
                error = '{0} {1}'.format(error, body['errorType'])
            raise LambdaCallError(error_type=error,
                                  message=body.get('errorMessage'),
                                  trace=body.get('stackTrace'))
        raise gen.Return(body)
