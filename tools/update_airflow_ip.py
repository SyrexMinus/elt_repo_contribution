import re
import sys
from pathlib import Path

tfvars_path = Path(sys.argv[1])
source_tfvars = tfvars_path.read_text()
updated_tfvars = re.sub(
    r'(?m)^([ \t]*airflow_allowed_cidr[ \t]*=[ \t]*)"[^"\n]*"',
    rf'\g<1>"{sys.argv[2]}/32"',
    source_tfvars,
)
if updated_tfvars != source_tfvars:
    tfvars_path.write_text(updated_tfvars)
