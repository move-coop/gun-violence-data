import civis
import logging
import os
from parsons import CivisClient
from canalespy import TMC_CIVIS_DATABASE, logger


def get_default_db_credential_for_civis_user(client=None):
    """
    Returns the default Civis database credential for the current Civis user.

    `Args:`
        client: object
            A civis Civis API client. If not specified, one will be created.

    `Returns:`
        object
            The Civis credential object
    """

    if not client:
        client = civis.APIClient()

    return client.credentials.list(type="Database", default=True)[0]


def get_schema_for_civis_user(default='tmc_scratch'):
    """
    Returns the Redshift schema for the current Civis user. It does this by matching the
    user's Civis groups to the "tmc.master_members_main" table.

    If no appropriate schema is found, "tmc_scratch" is returned as the default.

    `Args:`
        default_schema: str
            Schema to return if none can be found for the user

    `Returns:`
        str
            Name of the Redshift schema
    """

    # Get current user's groups
    client = civis.APIClient()
    user_info = client.users.list_me()
    user_group_ids = [g['id'] for g in user_info.groups]

    # Get the master members table. Use the Civis client instead of Parsons Redshift, so that
    # this will work even if the script doesn't have Redshift credentials.
    query = 'SELECT * FROM tmc.master_members_main'
    members_tbl = CivisClient(db=TMC_CIVIS_DATABASE).query(query)

    # Find a member row that matches one of the user's groups
    match_tbl = members_tbl.select_rows(
        lambda row: row.get('civis_group_id') and (row['civis_group_id'] in user_group_ids)
        )

    if match_tbl.num_rows > 0 and match_tbl[0]['schema']:
        return match_tbl[0]['schema']
    else:
        return default


def get_url_for_container_script(script_id):
    return f"https://platform.civisanalytics.com/#/jobs/redirect/JobTypes::ContainerDocker/{script_id}"  # noqa: E501


def get_url_for_custom_script(script_id):
    return f"https://platform.civisanalytics.com/spa/#/scripts/custom/{script_id}"


def update_success_email_for_multijob(script_name, job_results):
    """
    Updates the success email for the running Civis container script, to summarize the results
    from multiple jobs. It also prints the summary to the INFO log.

    If there is no running Civis container script (eg. if you're testing locally), it will just
    print the log.

    `Args:`
        script_name: str
            The name of your script, for display in email
        job_results: list
            List of job results, each being a dict containing a 'job_name', and an
            'exception' if one occurred.
    """

    civis_job_id = os.environ.get('CIVIS_JOB_ID')

    failures = [result for result in job_results if result.get('exception')]
    num_jobs = len(job_results)
    notifications = {}

    script_url = get_url_for_container_script(civis_job_id)
    body = f"## For full logs, go to the [container script]({script_url}).\n"

    if failures:
        subject = f"{script_name}: {len(failures)}/{num_jobs} jobs failed"
        logger.info(subject)

        body += "### Here's a summary of the job failures...\n\n"

        for failure in failures:
            e = failure['exception']
            failure_msg = (f"• Job '{failure['job_name']}' failed with exception of type "
                           f"{type(e).__name__}...\n\n{e}\n\n")
            logger.info(failure_msg)
            body += failure_msg

    else:
        subject = f"{script_name}: All {num_jobs} jobs succeeded"
        logger.info(subject)

    notifications['success_email_subject'] = subject
    notifications['success_email_body'] = body

    # Update the job's success email
    if civis_job_id:
        client = civis.APIClient()
        client.scripts.patch_containers(civis_job_id, notifications=notifications)


def trigger_civis_sql_job(
        sql,
        client=None,
        name="SQL job",
        db_id=TMC_CIVIS_DATABASE,
        email_addresses=None,
        success_on=True,
        success_email_subject="SQL job was successful",
        success_email_body="Here are your results: [link to file] ({{file_url}})",
        failure_on=True,
        archive=True,
        ):

    """
    Creates and runs a Civis SQL job, waiting for it to complete before returning.

    `Args:`
        sql: str
            The SQL query
        client: object
            A civis Civis API client. If not specified, one will be created.
        name: str
            The name of the job
        db_id: int
            The Civis database ID
        email_addresses: list
            Email addresses for success/failure emails. If None, will use the current
            Civis user's email address.
        success_on: bool
            Whether to trigger an email on success
        success_email_subject: str
            Subject of the success email
        success_email_body: str
            Body of the success email. Include `[link to file] ({{file_url}})` to generate
            a temporary link to a CSV with the query results.
        failure_on: bool
            Whether to trigger an email on failure
        archive: bool
            Whether to archive the job when completed

    `Returns:`
        object
            The Civis job object
    """

    if not client:
        client = civis.APIClient()

    if not email_addresses:
        user_info = client.users.list_me()
        email_addresses = [user_info['email']]

    db_credential = get_default_db_credential_for_civis_user()

    job = client.scripts.post_sql(
        name=name,
        sql=sql,
        remote_host_id=db_id,
        credential_id=db_credential.id,
        notifications={
            'success_on': success_on,
            'success_email_addresses': email_addresses,
            'success_email_subject': success_email_subject,
            'success_email_body': success_email_body,
            'failure_on': failure_on,
            'failure_email_addresses': email_addresses,
        }
    )

    logger.info(f"Created SQL job #{job.id} with name \"{name}\", running it...")

    try:
        run = client.scripts.post_sql_runs(job.id)

        # Wait on the run to finish
        poller = client.scripts.get_sql_runs
        poller_args = (job.id, run.id)
        polling_interval = 10
        future = civis.futures.CivisFuture(poller, poller_args, polling_interval)
        future.result()
    finally:
        # Archive the job, even if it failed
        if archive:
            client.scripts.put_sql_archive(job.id, True)

    logger.info("SQL job finished. Job success/failure email should have been sent.")

    return job


def upload_file_as_civis_script_outputs(filename, civis_job_id=None,
                                        civis_run_id=None):
    """Upload a file as Output to a Civis Script.

    Currently only supports container scripts. Note: The scripts must be
    running when this function is called.

    `Args:`
        civis_job_id: int
            The job id for a Civis container script.
        civis_run_id: int
            The run id for a Civis container script run.
    """
    job_id = civis_job_id if civis_job_id else os.getenv("CIVIS_JOB_ID")
    run_id = civis_run_id if civis_run_id else os.getenv("CIVIS_RUN_ID")

    if job_id and run_id:
        with open(filename, "r") as f:
            file_id = civis.io.file_to_civis(f, filename)

        client = civis.APIClient()
        client.scripts.post_containers_runs_outputs(
            job_id, run_id, 'File', file_id)


def upload_logs_as_civis_script_outputs(lgr):
    """Upload the logs files as Outputs to a Civis Script.

    If the script is running locally, then the function does nothing.

    `Args:`
        lgr: logging.Logger
            The logger object with file handlers.
    """
    for handle in lgr.handlers:
        if isinstance(handle, logging.FileHandler):
            log_file = handle.baseFilename

            upload_file_as_civis_script_outputs(log_file)
