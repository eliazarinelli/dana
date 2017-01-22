# dana - Data Analysis Projects

## Setup

Install [Anaconda](http://conda.pydata.org/docs/index.html).

Create a local environment with `conda`. The command below will create a local environment named `dana_env`:

```
conda create -n dana_env
```

Activate `dana_env` environment:
```
source activate dana_env
```

## Quickstart

Create some random orders executing
```
python example_orders.py
```
from `dana/parsers`.

Store the orders into an sqlite database running
```
populate_db.py
```
form `dana/dbadmin`.




## Mysql setup

Install [MySQL](http://dev.mysql.com/).

Install `pymysql` in the local environment:
```
conda install pymysql
```

Create a file in `dana/userconfig.py` with the following content:
```
USER_NAME = 'your_user_name'
USER_PWD = 'your_user_pwd'
HOST_NAME = 'local_or_remote_host'
DB_NAME = 'database_name'
```
This file should not be added to git to avoid sharing your credentials.