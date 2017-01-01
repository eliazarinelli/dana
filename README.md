# dana - Data Analysis Projects

#### Setup

Install [MySQL](http://dev.mysql.com/).

Install [Anaconda](http://conda.pydata.org/docs/index.html).

Create a local environment with python 3 from Anaconda distribution:

```
conda create -n env_name python=3 anaconda
```

Activate environment:
```
conda activate env_name
```

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
This file shoud not be added to git to avoid sharing your credentials. This file is included in `.gitignore`.