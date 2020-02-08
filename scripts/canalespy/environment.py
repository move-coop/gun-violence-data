import os
import json
from parsons.tools.credential_tools import decode_credential

TMC_CIVIS_DATABASE = 815
TMC_CIVIS_DATABASE_NAME = 'TMC'


def set_env_var(name, value, overwrite=False):
    """
    Set an environment variable to a value.
    `Args:`
        name: str
            Name of the env var
        value: str
            New value for the env var
        overwrite: bool
            Whether to set the env var even if it already exists
    """
    # Do nothing if we don't have a value
    if not value:
        return

    # Do nothing if env var already exists
    if os.environ.get(name) and not overwrite:
        return

    os.environ[name] = value


def setup_environment(redshift_parameter="REDSHIFT", aws_parameter="AWS", copper_parameter="COPPER", google_sheets_parameter="GOOGLE_SHEETS"):
    """
    Sets up environment variables needed for various common services used by our scripts.
    Call this at the beginning of your script.
    `Args:`
        redshift_parameter: str
            Name of the Civis script parameter holding Redshift credentials. This parameter
            should be of type "database (2 dropdown)" in Civis.
        aws_parameter: str
            Name of the Civis script parameter holding AWS credentials.
        copper_parameter: str
            Name of the Copper script parameter holding Copper user email and API key
        google_sheets_parameter: str
            Name of the Google Sheets script parameter holding a base64-encoded Google
            credentials dict
    """

    env = os.environ

    # Civis setup

    set_env_var('CIVIS_DATABASE', str(TMC_CIVIS_DATABASE))

    # Redshift setup

    set_env_var('REDSHIFT_PORT', '5432')
    set_env_var('REDSHIFT_DB', 'dev')
    set_env_var('REDSHIFT_HOST', env.get(f'{redshift_parameter}_HOST'))
    set_env_var('REDSHIFT_USERNAME', env.get(f'{redshift_parameter}_CREDENTIAL_USERNAME'))
    set_env_var('REDSHIFT_PASSWORD', env.get(f'{redshift_parameter}_CREDENTIAL_PASSWORD'))

    # AWS setup

    set_env_var('S3_TEMP_BUCKET', 'parsons-tmc')
    set_env_var('AWS_ACCESS_KEY_ID', env.get(f'{aws_parameter}_USERNAME'))
    set_env_var('AWS_SECRET_ACCESS_KEY', env.get(f'{aws_parameter}_PASSWORD'))

    # Copper setup

    set_env_var('COPPER_USER_EMAIL', env.get(f'{copper_parameter}_USERNAME'))
    set_env_var('COPPER_API_KEY', env.get(f'{copper_parameter}_PASSWORD'))

    # Google Sheets setup

    if f'{google_sheets_parameter}_PASSWORD' in env:
        key_dict = decode_credential(
            env.get(f'{google_sheets_parameter}_PASSWORD'),
            export=False
        )
        key_json = json.dumps(key_dict)
        set_env_var('GOOGLE_DRIVE_CREDENTIALS', key_json)
