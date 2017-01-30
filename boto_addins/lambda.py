import json

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from tornado import gen
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.httpclient import HTTPRequest

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest


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

    _url_template = ('https://lambda.'
                     '{region}.amazonaws.com/'
                     '{api_version}/functions/'
                     '{name}/invocations'
                     '?Qualifier={qualifier}')

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
            gif2video = Lambda('gif2video', credentials, <region>)
            result = yield gif2video(payload)
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
        self.url = self._url_template.format(region=self.region,
                                             api_version=self.API_VERSION,
                                             name=self.name,
                                             qualifier=qualifier)

    def _sign_request(self, request):
        """Sign request to AWS with SigV4Auth."""
        url = urlparse(request.url)
        path = url.path or '/'
        if url.query:
            querystring = '?' + url.query
        else:
            querystring = ''
        safe_url = "https://{location}{path}{query}".format(
            location=url.netloc.split(':')[0],
            path=path,
            query=querystring)

        aws_request = AWSRequest(method=request.method, url=safe_url,
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
        url = self.url
        headers = {'Content-Type': 'application/json'}
        request = HTTPRequest(url=url, headers=headers, method='POST',
                              body=json.dumps(payload))
        self._sign_request(request)
        response = yield self.client.fetch(request)
        error = response.headers.get('X-Amz-Function-Error', None)
        body = json.loads(response.body)
        if error:
            error_type = '{0} {1}'.format(error, body['errorType'])
            raise LambdaCallError(error_type=error_type,
                                  message=body['errorMessage'],
                                  trace=body['stackTrace'])
        raise gen.Return(body)
