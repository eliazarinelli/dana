# dana - Data Analysis Projects

## Basic setup

Install [Anaconda](http://conda.pydata.org/docs/index.html).

Create a local environment with python 3 from Anaconda distribution.
The command below will create a local environment named `dana_env`:

```
conda create -n dana_env
```

Activate `dana_env` environment:
```
source activate dana_env
```


#### Advanced setup

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