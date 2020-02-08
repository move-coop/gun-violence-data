import zipfile
import datetime
from parsons import Redshift, S3, VAN
import logging
import petl

logger = logging.getLogger()
logger.setLevel('INFO')

rs = Redshift()
s3 = S3()

"""
Prior to running this code, your score slots must already exist. To create score slots, simply
send a support request with committeeid and state.
The inputs to this function include three items:
    1. A Table Dictionary that should be formatted as such. The table can be a view, and must
       include a column for vanid and state.
    table_dict = {
        "table_name": "member.segment_scores",
        "vanid": "vb_smartvan_id",
        "state": "state"
    }
    2. A Score Dictionary includes each score column and the corresponding Score Name.
    To find score names, you can visit this URL when signed into
    SmartVAN: https://www.targetsmartvan.com/ScoreList.aspx?SideBarSelected=9
    Please use the exact score name as it appears on that page.
    score_dict = {
        "dem_score" : 'Democratic Score',
        "gop_score" : 'GOP Score',
        "ind_score" : 'Ind Score',
        "choice_score" : 'Choice Score'
    }
    3. Just a string with an email where VAN Score Emails will be sent to.
       Ex: 'elyse@movementcooperative.org'
    4. You must also submit your VAN API Keys in the following format.
    [
      {"org": "UWD", "state": "MD", "key": "XXX-XXX-XXX-XXX-XXXXXXX"},
      {"org": "UWD", "state": "TX", "key": "XXX-XXX-XXX-XXX-XXXXXXX"},
      {"org": "UWD", "state": "EA", "key": "XXX-XXX-XXX-XXX-XXXXXXX"}
    ]
      If you plan to run this in Civis, please create a custom credential with a multi-line
      password and drop the JSON thereself. Then, in your code, you can simply use:
      api_keys = os.environ['VAN_API_KEYS_PASSWORD']
"""

def create_zip_file(file_name, csv_path):
    # Create a zipped file

    try:
        import zlib
        compression = zipfile.ZIP_DEFLATED
    except:
        compression = zipfile.ZIP_STORED

    archive_name = f'{file_name}.zip'

    with zipfile.ZipFile(archive_name, "w") as z:
        z.write(csv_path, arcname=f'{file_name}.csv', compress_type=compression)

    return archive_name


def create_score_map(score_tbl, score_dict, score_table, tolerance):
    # Create a score map

    score_map = []

    for k, v in score_dict.items():

        # Calculate the average
        avg = petl.stats(score_tbl.table, k)[5]

        # Grab the score id
        for row in score_table:
            if row['name'] == v:
                score_id = row['scoreId']

        score = {'score_id': score_id,
                 'score_column': k,
                 'auto_average': avg,
                 'auto_tolerance': tolerance}

        score_map.append(score)

    return score_map


def post_scores(table_dict, score_dict, email, api_keys, tolerance=.02):

    # dealing with table dict
    currentDT = datetime.datetime.now().strftime("%Y%m%d")
    table = table_dict['table_name']
    split = rs.split_full_table_name(table)
    member = split[0]

    # determine how many states we need to loop through
    state_list = rs.query(f"select distinct {table_dict['state']} from {table}")
    states = [x['state'] for x in state_list]  # gets list of states
    logger.info(f'Will load scores to {len(states)} states.')

    # let's loop through states!
    for state in states:

        logger.info(f"Processing {state}.")

        # Instantiate the VAN Class with correct API key
        for x in api_keys:
            if x['state'] == state:
                key = x['key']
        van = VAN(api_key=key, db='MyVoters')

        # Get the state scores table from Redshift
        sql = f"select * from {table} where {table_dict['state']}='{state}'"
        state_scores = rs.query(sql)
        logger.info(f'Got {state} scores with {state_scores.num_rows} rows.')

        # Get list of score slot ids for this state
        score_names = [v for v in score_dict.values()]
        scores = van.get_scores()
        score_table = scores.select_rows(lambda row: row.name in score_names)

        # Create Score Map
        score_map = create_score_map(state_scores, score_dict, score_table, tolerance)
        logger.info(f'Created {state} score map.')

        # Zip Score Table
        file_name = f'{member}_{state}_{currentDT}'
        zip_path = create_zip_file(file_name, state_scores.to_csv())
        logger.info(f'Created {state} zip.')

        # Place on S3
        s3.put_file('vanscores', zip_path, zip_path)
        file_url = s3.get_url('vanscores', zip_path, expires_in=12000)
        logger.info('Compressed CSV uploaded to S3.')
        logger.debug(file_url)

        # Load Scores To VAN
        van.create_file_load_multi(file_name=f'{file_name}.csv',
                                   file_url=file_url,
                                   columns=state_scores.columns,
                                   id_column=table_dict['vanid'],
                                   id_type='VANID',
                                   score_map=score_map,
                                   email=email)
        logger.info(f'{state} scores posted to VAN. Check email for receipts.')
