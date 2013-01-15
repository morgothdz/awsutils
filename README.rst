Foreword
========
While I was working on multiple projects I found myself more than once lacking
a good python3.x amazon web services library, so I started scratching around the
subject.

The project I am working right now requires such a library, so the scratching
turned to a full blown project: awsutils. And because the open source community
was always such a great help, I humbly offer my work to the fellow programmers
who my be in the same need as I was.

For the beginning I will strictly add functionality only to cover my needs for
the primary project I am currently workin on and to improve reliability. 
Later I will expand more, based on users input and/or to cover other project 
needs.

Status
======
Pre-alpha, I am mostly planning and experimenting with the amazon protocols,
there are a number of things working. Hopefully in the next few days or couple
of weeks the most basic functionality required for S3 will be implemented and
ready to be hammered by tests.

Highlights
==========
#. Python3.x compatible amazon web services library
#. HTTP connection pool with "Connection: keep-alive"
#. Direct amazon api like access seconded by higher level access encapsulated 
   in various classes (ex: the generic s3client then the higher level s3.service, 
   s3.bucket and s3.object classes)
#. S3 support (limited yet)


Near Future Plans
=================
#. Solve the TODO's and exit Pre-alpha status :)
#. Support for HTTP Expect: 100-continue
#. HTTPS certificate check
#. Cleanup and bugfixes
#. Increase the generic AWSClient client reliability
#. Expanding S3 support and reliability
#. Amazon Simple DB support

Help Needed On
==============
#. Testing out functionality and report any bugs or problems.
#. Documentation
#. Automated tests
#. I am not planning python2.x features, there is the "boto" project more than
   enough for cover that road.
#. My english is pretty poor so feel free on helping me to the right track :)

Contributing
============
#. `Check for open issues <https://github.com/sanyi/awsutils/issues>`_ or open
   a fresh issue to start a discussion around a feature idea or a bug.
#. Fork the `awsutils repository on Github <https://github.com/sanyi/awsutils.git>`_
   to start making your changes.
#. Write a test which shows that the bug was fixed or that the feature works
   as expected.
#. Send a pull request and bug the maintainer until it gets merged and published.
   Make sure to add yourself to ``CONTRIBUTORS.txt`` ;).
