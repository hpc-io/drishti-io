# I/O Insights

To install I/O Insights, make sure you have Python 3 and first install the dependencies:

```
pip install -r requirements.txt
```

You can then run I/O Insights with the following options:

```
usage: insights.py [-h] [--issues] [--export] [--verbose] darshan

I/O Insights:

positional arguments:
  darshan     Input .darshan file

optional arguments:
  -h, --help  show this help message and exit
  --issues    Only displays the detected issues and hides the recommendations
  --export    Export the report as an HTML page
  --verbose   Display extended details for the recommendations
```