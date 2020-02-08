from parsons import Redshift, S3, VAN

def gather_dependent_sql(schema,name):

    query = f"""
            SELECT DISTINCT n_c.nspname AS schema
            ,c_c.relname AS view_name
    FROM pg_class c_p
    JOIN pg_depend d_p ON c_p.relfilenode = d_p.refobjid
    JOIN pg_depend d_c ON d_p.objid = d_c.objid
    JOIN pg_class c_c ON d_c.refobjid = c_c.relfilenode
    LEFT JOIN pg_namespace n_p ON c_p.relnamespace = n_p.oid
    LEFT JOIN pg_namespace n_c ON c_c.relnamespace = n_c.oid
    WHERE d_c.deptype = 'i'::"char"
    AND c_c.relkind = 'v'::"char"
    and n_p.nspname='{schema}'
    and c_p.relname='{name}'
    """

    dep = rs.query(query)
    logger.info(f"There are {dep.num_rows} depedent views...")
    dep_sql = []

    for x in dep:
        view_def = rs.get_view_definition(f"{x['schema']}.{x['view_name']}")
        dep_sql.append(view_def)

    logger.info(f"SQL Gathered!")

    return dep_sql

def run_dependent_sql(dep_sql):
    logger.info(f"Re-creating {len(dep_sql)} views...")
    for x in dep_sql:
        try:
            rs.query(x)
        except:
            None

    return None
