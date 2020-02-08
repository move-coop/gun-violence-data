import datetime
from parsons import Redshift
from canalespy import logger

def dedup_table(schema, table, order_by=None):

    rs = Redshift()
    stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    order = 'random()' if order_by is None else order_by

    dedup = '''
        alter table {schema}.{table}
        rename to temp_{stamp};

        create table {schema}.{table} as
        select * from
        (select *
        , row_number() over (partition by {partition} order by {order}) as dup
        from {schema}.temp_{stamp})
        where dup=1;

        alter table {schema}.{table}
        drop column dup;

        drop table {schema}.temp_{stamp} CASCADE;
    '''

    cols_tables = rs.get_columns(schema, table)
    cols = [x for x in cols_tables]

    partition = ', '.join(cols)

    rs.query(dedup.format(schema=schema,table=table,partition=partition,stamp=stamp, order=order))
    logger.info(f'Finished deduping {schema}.{table}...')
