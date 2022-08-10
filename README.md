<p align="center">
  <img src="https://github.com/hpc-io/io-insights/blob/master/images/drishti-logo.png?raw=true" alt="Drishti"/>
</p>

# Drishti: I/O Insights for All

To install Drishti, make sure you have Python 3 and first install the dependencies:

```
pip install -r requirements.txt
```

You can then run Drishti with the following options:

```
usage: drishti.py [-h] [--issues] [--html] [--svg] [--verbose] [--code] darshan

Drishti:

positional arguments:
  darshan     Input .darshan file

optional arguments:
  -h, --help  show this help message and exit
  --issues    Only displays the detected issues and hides the recommendations
  --html      Export the report as an HTML page
  --svg       Export the report as an SVG image
  --verbose   Display extended details for the recommendations
  --code      Display insights identification code
```

By default Drishti will generate an overview report in the console with recommendations:

<p align="center">
  <img src="https://github.com/hpc-io/io-insights/blob/master/images/sample-io-insights.svg?raw=true" alt="Drishti"/>
</p>

You can also only list the issues detected by Drishti with `--issues`:

<p align="center">
  <img src="https://github.com/hpc-io/io-insights/blob/master/images/sample-io-insights-issues.svg?raw=true" alt="Drishti"/>
</p>

You can also enable the verbose mode with `--verbose` to visualize solution snippets:

<p align="center">
  <img src="https://github.com/hpc-io/io-insights/blob/master/images/sample-io-insights-verbose.svg?raw=true" alt="Drishti"/>
</p>
