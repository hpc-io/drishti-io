import argparse

parser = argparse.ArgumentParser(
    description='Drishti: '
)

parser.add_argument(
    'log_path',
    help='Input .darshan file or recorder folder'
)

parser.add_argument(
    '--issues',
    default=False,
    action='store_true',
    dest='only_issues',
    help='Only displays the detected issues and hides the recommendations'
)

parser.add_argument(
    '--html',
    default=False,
    action='store_true',
    dest='export_html',
    help='Export the report as an HTML page'
)

parser.add_argument(
    '--svg',
    default=False,
    action='store_true',
    dest='export_svg',
    help='Export the report as an SVG image'
)

parser.add_argument(
    '--light',
    default=False,
    action='store_true',
    dest='export_theme_light',
    help='Use a light theme for the report when generating files'
)

parser.add_argument(
    '--size',
    default=False,
    dest='export_size',
    help='Console width used for the report and generated files'
)

parser.add_argument(
    '--verbose',
    default=False,
    action='store_true',
    dest='verbose',
    help='Display extended details for the recommendations'
)

parser.add_argument(
    '--threshold',
    default=False,
    action='store_true',
    dest='thold',
    help='Display all thresholds used for the report'
)

parser.add_argument(
    '--code',
    default=False,
    action='store_true',
    dest='code',
    help='Display insights identification code'
)

parser.add_argument(
    '--backtrace',
    default=False,
    action='store_true',
    dest='backtrace',
    help='Enable DXT insights and backtrace'
)

parser.add_argument(
    '--path',
    default=False,
    action='store_true',
    dest='full_path',
    help='Display the full file path for the files that triggered the issue'
)

parser.add_argument(
    '--csv',
    default=False,
    action='store_true',
    dest='export_csv',
    help='Export a CSV with the code of all issues that were triggered'
)

parser.add_argument(
    '--json', 
    default=False, 
    dest='json',
    help=argparse.SUPPRESS
)

parser.add_argument(
    '--split',
    default=False,
    action='store_true',
    dest='split_files',
    help='Split the files and generate report for each file'
)

parser.add_argument(
    '--config',
    default=False,
    dest='config',
    help='Enable thresholds read from json file'
)

args = parser.parse_args()
