from jinja2 import Template
from jinja2.utils import concat
import gzip


def read_file(path):
    compression = 'gz' if path[-3:] == '.gz' else None

    open_func = {
        'gz': gzip.open,
        None: open,
    }
    with open_func[compression](path, 'r') as fp:
        return fp.read()


def template_blocks_to_dict(path, vars=None, use_default_env=True):
    env = {
        "block_start_string": "/* {%",
        "block_end_string": "%} */",
    }

    if use_default_env:
        env = {}

    sql = read_file(path)
    template = Template(sql, **env)
    context = template.new_context(vars)

    data = {name: concat(block_func(context)).strip()
            for name, block_func in template.blocks.items()}

    # logger.info(f"Found {len(data)} blocks in {path}")

    return data
