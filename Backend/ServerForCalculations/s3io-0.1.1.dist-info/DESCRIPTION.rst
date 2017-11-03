====
s3io
====

About
-----

s3io is a minimalistic python module which provides file object access to data on S3.

All data manipulations are done via temporary local files - so you will actually work with local temp file which is a file object with all its methods supported.

Just keep in mind these facts:

1. Reading: the whole file will be downloaded to local temporary location at entering the context.
2. Writing: temp file will be provided at entering the context. Actual saving to S3 will be performed at exit from context. Thus such methods as ``flush`` will not influence the process of saving to S3.

Dependencies
------------

- `boto <https://github.com/boto/boto>`_

Examples
--------

s3io is intended to be used via context manager only.

There are three ways to provide access to s3:

1. Directly providing ``s3_connection``.
2. Providing credentials: ``aws_access_key_id`` and ``aws_secret_access_key``.
3. Providing ``profile_name``. This method is recommended to use. See `boto docs <http://boto.readthedocs.org/en/latest/boto_config_tut.html>`_ for more info.

Reading file using existing S3 connection::

        import s3io

        s3 = boto.connect_s3()
        with s3io.open('s3://<bucket>/<key>', s3_connection=s3) as s3_file:
            contents = s3_file.read()

Reading file using credentials::

        credentials = dict(
            aws_access_key_id='<ACCESS_KEY>',
            aws_secret_access_key='<SECRET_KEY>',
        )
        with s3io.open('s3://<bucket>/<key>', **credentials) as s3_file:
            contents = s3_file.read()

Reading file using profile::

        with s3io.open('s3://<bucket>/<key>', profile_name='<profile>') as s3_file:
            contents = s3_file.read()

Writing file using profile::

        with s3io.open('s3://<bucket>/<key>', mode='w', profile_name='<profile>') as s3_file:
            s3_file.write('Some data.')

Exceptions
----------

Possible exceptions:

1. s3io.BucketNotFoundError
2. s3io.KeyNotFoundError
3. s3io.UrlParseError


