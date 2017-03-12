# dana - Data Analysis Projects

## Setup

Install [MongoDB](https://www.mongodb.com) version 3.4.2 or more recent.

Install [Anaconda](http://conda.pydata.org/docs/index.html).

Create a local environment with `conda`. The command below will create a local environment named `dana_env`:

```
conda create -n dana_env
```

Activate `dana_env` environment:
```
source activate dana_env
```

Intall [Bokeh](http://bokeh.pydata.org/en/latest/) in the local environment:
```
conda install bokeh
```

Install [pymongo](https://api.mongodb.com/python/current/) in the local environmet:
```
conda install pymongo
```

## Quickstart

Open the notebook `notebooks/quickstart.ipynb` and follow the steps.

## Documentation

Waiting for an official documentation, plese have a look to the notebooks:

* `notebooks/hts_api.ipynb`
* `notebooks/reservoir_api.ipynb`

## Dataviz

Go to the `apps` folder and run
```
bokeh serve intraday.py
```

Then from your browser go to `http://localhost:5006/intraday` and play around.

