from contextlib import contextmanager
import re
from tempfile import NamedTemporaryFile

import boto


__all__ = [
    'open',
    'BucketNotFoundError',
    'KeyNotFoundError',
    'UrlParseError',
]


S3_URL_REGEX = '(?P<scheme>s3:\/\/)(?P<bucket>[^\/]+)\/(?P<key>.+)'
re_s3_url = re.compile(S3_URL_REGEX)


class UrlParseError(Exception):
    pass


class KeyNotFoundError(Exception):
    pass


class BucketNotFoundError(Exception):
    pass


def url_split(s3_url):
    """Split S3 URL and return a tuple of (bucket, key).

    S3 URL is expected to be of "s3://<bucket>/<key>" format.
    """

    assert isinstance(s3_url, str)

    match = re_s3_url.match(s3_url)
    if not match:
        raise UrlParseError('Error parsing S3 URL: "%s"' % s3_url)

    parts = match.groupdict()
    return (parts['bucket'], parts['key'])


@contextmanager
def open(s3_url, mode='r', s3_connection=None, **kwargs):
    """Open S3 url, returning a File Object.

    S3 connection:
        1. Can be specified directly by `s3_connection`.
        2. `boto.connect_s3` will be used supplying all `kwargs`.
           - `aws_access_key_id` and `aws_secret_access_key`.
           - `profile_name` - recommended.
              See:
              http://boto.readthedocs.org/en/latest/boto_config_tut.html
    """

    connection = s3_connection or boto.connect_s3(**kwargs)

    bucket_name, key_name = url_split(s3_url)

    try:
        bucket = connection.get_bucket(bucket_name)
    except boto.exception.S3ResponseError:
        raise BucketNotFoundError('Bucket "%s" was not found.' % bucket_name)

    f = NamedTemporaryFile()
    try:
        if 'w' in mode.lower():
            s3_key = bucket.new_key(key_name)

            yield f
            f.seek(0)
            s3_key.set_contents_from_file(f)
        else:
            s3_key = bucket.get_key(key_name)

            if not s3_key:
                raise KeyNotFoundError('Key "%s" was not found.' % s3_url)

            s3_key.get_file(f)
            f.seek(0)
            yield f
    finally:
        f.close()
