import os
import re
import sys
from pathlib import Path

tfvars_path = Path(sys.argv[1])
tfvars = tfvars_path.read_text()
for name in ("airflow_image", "silver_image"):
    tfvars = re.sub(
        rf'(?m)^({name}[ \t]*=[ \t]*)"[^"\n]*"',
        lambda match: f'{match[1]}"{os.environ[name.upper()]}"',
        tfvars,
    )
tfvars_path.write_text(tfvars)
