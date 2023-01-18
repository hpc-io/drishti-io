<p align="center">
  <img src="https://github.com/hpc-io/io-insights/blob/master/images/drishti-logo.png?raw=true" alt="Drishti"/>
</p>

# Drishti: I/O Insights for All

Drishti is a command-line tool to guide end-users in optimizing I/O in their applications by detecting typical I/O performance pitfalls and providing a set of recommendations. You can get Drishti directly from pip:

```
pip install drishti-io
```

To install Drishti from scratch, make sure you have Python 3 and first install the dependencies:

```
pip install -r requirements.txt
pip install .
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

You can also use our Docker image:

```
docker run --rm --mount type=bind,source="$(PWD)",target=/drishti drishti sample/jlbez_8a_benchmark_write_parallel_id1321662_8-21-5892-15802854900629188750_106.darshan
```


You can also use a Docker image already pre-configured with all dependencies to run Drishti:

```
docker pull hpcio/drishti
```

Since we need to provide a Darshan log file as input, make sure you are mounting your current directory in the container and removing the container after using it. You can pass the same arguments described above, after the container name (drishti).

```
docker run --rm --mount \
    type=bind,source="$(PWD)",target="/drishti" \
    drishti <FILE>.darshan
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

---

### Copyright Notice

Drishti Copyright (c) 2022, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software, please contact Berkeley Lab's Intellectual Property Office at IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department of Energy and the U.S. Government consequently retains certain rights.  As such, the U.S. Government has been granted for itself and others acting on its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the Software to reproduce, distribute copies to the public, prepare derivative works, and perform publicly and display publicly, and to permit others to do so.

