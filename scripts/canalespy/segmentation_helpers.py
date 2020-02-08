from parsons import Redshift
from canalespy import logger, CanalesParallel
from joblib import delayed

# We should be extra careful changing permissions on these schemas
PROTECTED_SCHEMAS = ['tmc', 'tmc-scratch']


def get_member_lookup_table(table, col):
    """Get the lookup table for desired tool.
        This returns the master lookup table, which is used to
        determine what data belongs to what member.
    """
    logger.info("Getting lookup table...")
    rs = Redshift()

    if col=='van_committees':
        q = f"""
            select schema
            , coalesce(redshift_group,'tmc') as redshift_group
            , {col}
            , code
            from {table}
            where {col} is not NULL and code in
            (select code from tmc.master_members_van
            where uses_redshift ilike 'yes')
        """
    else:
        q = f"""
            select schema
            , coalesce(redshift_group,'tmc') as redshift_group
            , {col}
            , code
            from {table}
            where {col} is not NULL
        """
    tbl = rs.query(q)
    logger.info(f"Found {tbl.num_rows} members")
    return tbl


def get_id_column_name(tables, search_columns):
    """
    Given a list of Redshift tables, figures out the ID column for each table.
    `Args:`
        tables: list
            List of tables, from Redshift.get_tables()
        search_columns: list
            List of possible ID column names
    `Returns:`
        Dict that maps table names to ID columns
    """
    tbl_and_id_col = {}
    for table in tables:
        logger.info(f"Retrieving {table['schemaname']}.{table['tablename']}")
        # To Do: Have a Parsons method the lists columns in a table
        sql = f"select * from {table['schemaname']}.{table['tablename']} limit 1"
        table_cols = Redshift().query(sql).columns
        for col in search_columns:
            if col in table_cols:
                tbl_and_id_col[table['tablename']] = col
                break
            else:
                tbl_and_id_col[table['tablename']] = None

    return tbl_and_id_col


def create_schema_for_segmenting(redshift, schema, redshift_group):
    if redshift_group and (schema in PROTECTED_SCHEMAS):
        logger.warning(f"Attempting to change permissions on a protected schema {schema}."
                       "This will be ignored.")
    else:
        redshift.create_schema_with_permissions(schema, redshift_group)


def _segment_redshift_query_to_member(dest_schema, dest_table_name, redshift_group, query,
                                      if_exists, distkey=None, sortkey=None):

    dest_table = f"{dest_schema}.{dest_table_name}"
    job_name = f"Segmenting query results to {dest_table}..."
    result = {'job_name': job_name}

    logger.info(f"{job_name} starting...")

    try:
        rs = Redshift()

        create_schema_for_segmenting(rs, dest_schema, redshift_group)

        rs.populate_table_from_query(query, dest_table, if_exists=if_exists, distkey=distkey,
                                     sortkey=sortkey)

        rs.query(f"grant select on table {dest_table} to group {redshift_group}")

        logger.info(f"{job_name} finished")

    except Exception as e:
        # Catch and return all exceptions
        result['exception'] = e

        logger.info(f"{job_name} failed with exception of type "
                    f"{type(e).__name__}...\n{e}\n")

    finally:
        return result


def segment_redshift_query_to_members(member_segmentation_list, if_exists,
                                      distkey=None, sortkey=None):
    """
    Segments Redshift data to members by running a per-member query and copying the results to
    per-member tables.

    Use this when you do not have all your source data in a single table, and you need a more
    complex query to grab it (eg. when you need to join tables). Otherwise consider using
    ``segment_redshift_table_to_members()``.

    The members are specified via the argument ``member_segmentation_list``. It should be a list
    of dicts, each dict specifying a member's destination schema, destination table, Redshift
    group, and the query. For example:

    query = '''
        select * from tmc.vendor_actions left join tmc.vendor_orgs using (campaign_id)
        where org_id = {org_id}
    '''

    member_segmentation_list = [
        {
          'schema': 'indivisible',
          'table_name': 'some_vendor_actions',
          'redshift_group': 'indivisible',
          'query': query.format(org_id=456),
        },
        {
          'schema': 'sua',
          'table_name': 'some_vendor_actions',
          'redshift_group': 'sua',
          'query': query.format(org_id=123),
        },
        ... etc ...
    ]

    For each member, the schema will be created (if it doesn't already exist), and the member
    will be granted permissions to read their segmented table.

    IMPORTANT - This helper will continue processing even if one or more of the segmentation jobs
    fails. In order to see which jobs actually succeeded or failed, you'll need to examine the
    list of job results that is returned (see details below under `Returns:`).

    `Args:`
        member_segmentation_list: list
            See above
        if_exists: str
            If the destination tables already exist, either ``fail``, ``append``, ``drop``,
            or ``truncate`` the table.
        distkey: str
            The column to use as the distkey for the destination table (if the table doesn't
            exist).
        sortkey: str
            The column to use as the sortkey for the destination table (if the table doesn't
            exist).
    `Returns:`
        list
            List of results from the segmentation jobs, each being a dict containing a 'job_name',
            and an 'exception' if one occurred.
    """

    # The jobs are almost entirely constrained by waiting for Redshift, and writing to different
    # tables. So the more jobs, the better.
    n_jobs = len(member_segmentation_list)

    parallel_jobs = [
        delayed(_segment_redshift_query_to_member)(
            member['schema'], member['table_name'], member['redshift_group'],
            member['query'], if_exists=if_exists, distkey=distkey, sortkey=sortkey
        )
        for member in member_segmentation_list
    ]

    return CanalesParallel(n_jobs=n_jobs)(parallel_jobs)


def _segment_redshift_table_to_member(source_table, dest_schema, dest_table_name, redshift_group,
                                      where_clause, if_exists):

    dest_table = f"{dest_schema}.{dest_table_name}"
    job_name = f"Segmenting {source_table} to {dest_table}"
    result = {'job_name': job_name}

    logger.info(f"{job_name} starting...")

    try:
        rs = Redshift()

        create_schema_for_segmenting(rs, dest_schema, redshift_group)

        where_clause_full = f"WHERE {where_clause}"
        rs.duplicate_table(source_table, dest_table, where_clause=where_clause_full,
                           if_exists=if_exists)

        rs.query(f"grant select on table {dest_table} to group {redshift_group}")

        logger.info(f"{job_name} finished")

    except Exception as e:
        # Catch and return all exceptions
        result['exception'] = e

        logger.info(f"{job_name} failed with exception of type "
                    f"{type(e).__name__}...\n{e}\n")

    finally:
        return result


def segment_redshift_table_to_members(source_table, member_segmentation_list, if_exists):
    """
    Segments a source Redshift table by copying rows relevant to particular members into various
    destination tables.

    Use this when you already have a Redshift table with all the data you need to segment.
    If you need to pull the source data from a *view* instead of a table, or join with other
    tables, or use a custom query for whatever reason, use ``segment_redshift_query_to_members()``
    instead.

    The members are specified via the argument ``member_segmentation_list``. It should be a list
    of dicts, each dict specifying a member's destination schema, destination table, Redshift
    group, and WHERE clause for filtering the source table. For example:

    member_segmentation_list = [
        {
          'schema': 'indivisible',
          'table_name': 'some_vendor_actions',
          'redshift_group': 'indivisible',
          'where_clause': "org_id = 456"
        },
        {
          'schema': 'sua',
          'table_name': 'some_vendor_actions',
          'redshift_group': 'sua',
          'where_clause': "org_id = 123"
        },
        ... etc ...
    ]

    For each member, the schema will be created (if it doesn't already exist), and the member
    will be granted permissions to read their segmented table.

    IMPORTANT - This helper will continue processing even if one or more of the segmentation jobs
    fails. In order to see which jobs actually succeeded or failed, you'll need to examine the
    list of job results that is returned (see details below under `Returns:`).

    `Args:`
        source_table: str
            The source table (including schema, ie. "my_schema.my_table")
        member_segmentation_list: list
            See above
        if_exists: str
            If the destination tables already exist, either ``fail``, ``append``, ``drop``,
            or ``truncate`` the table.
    `Returns:`
        list
            List of results from the segmentation jobs, each being a dict containing a 'job_name',
            and an 'exception' if one occurred.
    """

    # The jobs are almost entirely constrained by waiting for Redshift, and writing to different
    # tables. So the more jobs, the better.
    n_jobs = len(member_segmentation_list)

    parallel_jobs = [
        delayed(_segment_redshift_table_to_member)(
            source_table, member['schema'], member['table_name'], member['redshift_group'],
            member['where_clause'], if_exists=if_exists
        )
        for member in member_segmentation_list
    ]

    return CanalesParallel(n_jobs=n_jobs)(parallel_jobs)


def segment_redshift_table_to_member_views(source_table, member_segmentation_list):
    """
    Segments a source Redshift table by creating views for each member.

    Use this when you already have a Redshift table with all the data you need to segment, and
    you'd rather create views than tables.

    If you need to pull the source data from a *view* instead of a table, or join with other
    tables, or use a custom query for whatever reason, use ``segment_redshift_query_to_members()``
    instead.

    The members are specified via the argument ``member_segmentation_list``. It should be a list
    of dicts, each dict specifying a member's destination schema, destination view, Redshift
    group, and WHERE clause for filtering the source table. Note that the field for specifying
    the view is called "table_name", just for consistency with the other segmentation methods.
    For example:

    member_segmentation_list = [
        {
          'schema': 'indivisible',
          'table_name': 'some_vendor_actions',
          'redshift_group': 'indivisible',
          'where_clause': "org_id = 456"
        },
        {
          'schema': 'sua',
          'table_name': 'some_vendor_actions',
          'redshift_group': 'sua',
          'where_clause': "org_id = 123"
        },
        ... etc ...
    ]

    For each member, the schema will be created (if it doesn't already exist), and the member
    will be granted permissions to read their segmented view.

    `Args:`
        source_table: str
            The source table (including schema, ie. "my_schema.my_table")
        member_segmentation_list: list
            See above
    `Returns:`
        None
    """

    rs = Redshift()

    for member in member_segmentation_list:

        schema = member['schema']
        view = rs.combine_schema_and_table_name(schema, member['table_name'])
        where_clause = member['where_clause']
        redshift_group = member['redshift_group']

        view_count = rs.query(f"select count(*) from {source_table} where {where_clause}")
        if view_count[0]['count'] > 0:
            logger.info(f"Creating view {view} for segmentation...")
            create_schema_for_segmenting(rs, schema, redshift_group)
            rs.query(f"drop view if exists {view}; create view {view} as select * from {source_table} where {where_clause};")
            rs.query(f"grant select on {view} to group {redshift_group}")
        else:
            logger.info(f"{view} would have zero rows, so not creating...")

    return None

def segment_redshift_query_to_views(member_segmentation_list):
    """
    Segments Redshift data to members by running a per-member query and copying the results to
    per-member views.

    The members are specified via the argument ``member_segmentation_list``. It should be a list
    of dicts, each dict specifying a member's destination schema, destination view, Redshift
    group, and the query. For example:

    query = '''
        select * from tmc.vendor_actions left join tmc.vendor_orgs using (campaign_id)
        where org_id = {org_id}
    '''

    member_segmentation_list = [
        {
          'schema': 'indivisible',
          'view_name': 'some_vendor_actions',
          'redshift_group': 'indivisible',
          'query': query.format(org_id=456),
        },
        {
          'schema': 'sua',
          'view_name': 'some_vendor_actions',
          'redshift_group': 'sua',
          'query': query.format(org_id=123),
        },
        ... etc ...
    ]

    For each member, the schema will be created (if it doesn't already exist), and the member
    will be granted permissions to read their segmented view.

    IMPORTANT - This helper will continue processing even if one or more of the segmentation jobs
    fails. In order to see which jobs actually succeeded or failed, you'll need to examine the
    list of job results that is returned (see details below under `Returns:`).

    `Args:`
        member_segmentation_list: list
            See above
    `Returns:`
        list
            List of results from the segmentation jobs, each being a dict containing a 'job_name',
            and an 'exception' if one occurred.
    """

    rs = Redshift()

    for member in member_segmentation_list:

        schema = member['schema']
        dest_view = rs.combine_schema_and_table_name(schema, member['view_name'])
        redshift_group = member['redshift_group']
        start_query = f"drop view if exists {dest_view}; create view {dest_view} as "

        view_count = rs.query(f"select count(*) from ({member['query']})")
        if view_count[0]['count'] > 0:
            logger.info(f"Creating view {dest_view} for segmentation...")
            create_schema_for_segmenting(rs, schema, redshift_group)
            rs.query(f"{start_query} {member['query']}")
            rs.query(f"grant select on {dest_view} to group {redshift_group}")
        else:
#             logger.info(f"{dest_view} would have zero rows, so not creating...")
            logger.info(f"{dest_view} would have zero rows, so deleting to be safe...")
            rs.query(f"drop view if exists {dest_view}")

    return None
