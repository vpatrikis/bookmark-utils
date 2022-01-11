Bookmark Utils
==============================================================================

[Use a few sentence to describe what is this tool, what problem it trying to solve, who is the potential user]

.. contents:: Table of Content
    :depth: 1
    :local:

Usage (or Tutorial)
------------------------------------------------------------------------------

**Before State**

- Prepare AWS resources
- ...

**Run Sample Code**

- [how to set up execution environment?]
- [sample codes]
- [explain what does the code do]
- [explain the expected output]

**After State**

- ...


Dev Runbook
------------------------------------------------------------------------------

1. Setup Virtualenv:

.. code-block:: bash

    # Create a Python virtual environment for dev / test
    $ virtualenv -p python3.8 venv

    # Enter virtualenv
    $ source ./venv/bin/activate

    # pip install this library and dependencies
    $ pip install -e .

2. Run Tests:

.. code-block:: bash

    # pip install test dependencies
    # NOTE YOU MAY NEED TO RE-ENTER virtualenv
    $ pip install -r requirements-test.txt

    # run unit test and code coverage test
    $ pytest tests -s --cov=bookmark_utils --cov-report term-missing --cov-report "annotate:/Users/sanhehu/Documents/GitHub/bookmark-utils/.coverage.annotate"

3. Package and Publish:

.. code-block:: bash

    # pip install development dependencies
    # NOTE YOU MAY NEED TO RE-ENTER virtualenv
    $ pip install -r requirements-dev.txt

    # build artifacts locally
    $ bash ./bin/build.sh

    # publish to https://pypi.org
    $ bash ./bin/publish.sh

1. Then create a release branch ``release/x.y.z`` that match the version.
2. Tag the repo from this branch using naming convention ``x.y.z``.
3. Create a GitHub Release that name matching this version using naming convention ``x.y.z``, and upload the ``.whl`` file to the Release.
