from parsons.utilities import files
from jinja2 import Template
from jinja2.utils import concat


def template_blocks_to_dict(path, vars=None, use_env=None):
    """
    Return a dictionary of blocks from a template (based on jinja2).

    Example:
        `queries.sql`::

            /* {% block query_1 %} */

            select * from schema.table

            /* {% endblock query_1 %} */

            /* {% block query_2 %} */

            select * from schema.table
            join schema.other_table using({{id}})

            /* {% endblock query_2 %} */


        .. code-block:: python

            >>> template_blocks_to_dict(
                "queries.sql", {"id": "join_id"}, "sql-mlc")
            {'query_1': 'select * from schema.table', 'query_2': 'select * from
             schema.table\njoin schema.other_table using(join_id)'}

    `Args:`
        path: str
            The path to the file that contains the template blocks.
        vars: dict
            Dictionary of variables to pass to the template. Any variables not
            supplied will have not text output in the resulting dictionary.
            Note: Within the jinja2 template, varables use two sets of
            brackets, e.g. {{ var_name }}
        use_env: str
            Use a custom predefined set of start and end block strings.
            `sql`: `-- {%` and `%}`
            `sql-mlc`: `/* {%` and `%}`
    `Returns:`
        dict
            Dictionary of blocks names and their contents.
    """
    envs = {
        "sql": {
            "block_start_string": "-- {%",
            "block_end_string": "%}",
        },
        "sql-mlc": {
            "block_start_string": "/* {%",
            "block_end_string": "%} */",
        },
    }

    env = envs[use_env] if use_env else {}

    sql = files.read_file(path)
    template = Template(sql, **env)
    context = template.new_context(vars)

    data = {name: concat(block_func(context)).strip()
            for name, block_func in template.blocks.items()}

    # logger.info(f"Found {len(data)} blocks in {path}")

    return data
