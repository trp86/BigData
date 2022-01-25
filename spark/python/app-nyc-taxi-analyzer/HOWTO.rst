=======
How To
=======

Application **app-nyc-taxi-analyzer** is a data pipeline developed using Apache Spark and Python.

-------------------------
Running the project
-------------------------

Before running the commands using `make`, ensure that:

* `poetry` is `installed <https://python-poetry.org/docs/#installation>`_.
* python 3.8+ is installed

.. code:: bash

    git clone https://github.com/trp86/BigData/tree/master/spark/python/app-nyc-taxi-analyzer.git
    cd app-nyc-taxi-analyzer
    poetry --version
    poetry install
    poetry run spark-submit run.py dev

Makefile
---------

Try out the :code:`make <command>`.

+-----------------+---------------------------------------------------+
| <command>       |  description                                      |
+=================+===================================================+
| help            | lists the options available with make             |
+-----------------+---------------------------------------------------+
| lint            | flake8 lint and black code style                  |
+-----------------+---------------------------------------------------+
| mypy            | static analysis with mypy                         |
+-----------------+---------------------------------------------------+
| bandit          | discover common security issues                   |
+-----------------+---------------------------------------------------+
| test            | run tests in the current environment              |
+-----------------+---------------------------------------------------+
| coverage        | generate coverage report                          |
+-----------------+---------------------------------------------------+
| sphinx.html     | build html documentation                          |
+-----------------+---------------------------------------------------+
| submit          | submits the job without any extra dependency      |
+-----------------+---------------------------------------------------+
| submit_with_dep*| submits the job with packaged dependency          |
+-----------------+---------------------------------------------------+
| run-pipeline    | runs the full-CI/CD workflow locally              |
+-----------------+---------------------------------------------------+

\* uses the :code:`docker-compose` command. Ensure that your docker-compose version is 1.29.0+

-----------------
Project structure
-----------------

::

    app-nyc-taxi-analyzer
    ├── .github                           # GitHub actions
    ├── .flake8                           # Rules defined for flake8 linting
    ├── HOWTO.rst                         # How to get started
    ├── changelog.rst                     # History of changes that are done to this project
    ├── docker-compose.yml                # Docker Compose file
    ├── Makefile                          # Make File
    ├── pyproject.ml                      # Python dependencies used in this project
    ├── README.rst                        # Information about the project
    ├── run.py                            # Entry point to spark job
    ├── docker                            # Package dependencies
    |   └── package_dependency.Dockerfile # Docker installation file    
    ├── docs                              # Sphinx documentation
    │   └── source                        # Configuration for sphinx
    ├── src                               # Source code
    │   └── jobs                          # Sources code related to ETL job
    │       └── utils                     # Utilities to assist ETL job
    │   └── resources                     # Configuration files used in pyspark job    
    └── tests                             # Unit and Integration tests
        ├── integration                   # Integration tests
        ├── unit                          # Unit tests
        └── resources                     # Resources used for unit and integration tests
           └── conf                       # Configuration files used for unit and integration tests 
           └── data                       # Test data files used for unit and integration tests