from logging import INFO
from logging import basicConfig
from logging import getLogger
from sys import stdout

basicConfig(
    level=INFO,
    format='%(asctime)s - %(message)s',
    stream=stdout
)
logger = getLogger(__name__)
