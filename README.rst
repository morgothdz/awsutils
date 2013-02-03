**awsutils**, AWS (Amazon Web Services) python3 library
=======================================================

Foreword
--------
While there already exists other python AWS library (boto) it lacks  
**python3** support. For my projects (and I trust that for others too)
**Python3** support and some asynchronous handling is a must. I hope **awsutils**
will be and useful addition to the python world.

Highlights
----------
#. Python3 compatible, it's developed using python3.2
#. Amazon **SQS** low and high level API with the most usual functions implemented
#. Amazon **SimpleDB** low level API
#. Amazon **SimpleDB** low level API **asynchronous** implementation based on **tornado**,
   with the most usual functions implemented
#. Amazon **S3** low level API (incomplete) and higher level API (incomplete), 
   still the S3Bucket contains two high level powerful file manipulation functions, making 
   large file upload and download secure and transparent.
#. HTTP connection pool with "Connection: keep-alive"
#. Structured on two levels, a low level translating directly the amazon API's, 
   and a higher one, where things are organized in classes etc..

Status
------
Slowly reaching the alpha status, hopefully in the next couple of weeks the structure will 
freeze.

Near Future Plans
-----------------
#. Solve the TODO's and reach the alpha status
#. Clean-up and bugfixes
#. Increase the generic AWSClient client reliability and power, ex: Support for HTTP 
   Expect: 100-continue, HTTPS certificate check, etc.
#. Add more funcionality

Help Needed On
--------------
#. Testing out functionality and report any bugs or problems.
#. Documentation
#. Automated tests
#. Correct my language mistakes.
#. I am not planning python2.x features, there is the **boto** more than
   enough for that path.

Contributing
------------
#. `Check for open issues <https://github.com/sanyi/awsutils/issues>`_ or open
   a fresh issue to start a discussion around a feature idea or a bug.
#. Fork the `awsutils repository on Github <https://github.com/sanyi/awsutils.git>`_
   to start making your changes.
#. Write a test which shows that the bug was fixed or that the feature works
   as expected.
#. Send a pull request and bug the maintainer until it gets merged and published.
   Make sure to add yourself to ``CONTRIBUTORS.txt`` ;).
