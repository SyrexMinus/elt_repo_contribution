import os
import sys
from pathlib import Path
from string import Template

template = Template(
    (Path(__file__).with_name("usage_templates") / sys.argv[1]).read_text()
)
sys.stdout.write(template.substitute(os.environ))
