from canalespy.environment import setup_environment, TMC_CIVIS_DATABASE, TMC_CIVIS_DATABASE_NAME
from joblib import Parallel
import logging


# Define the default logging config for Canales scripts
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')


# When writing parallelized code, use this variant of joblib.Parallel. It
# ensures we use the optimal number of jobs for Civis's containers, and will
# allow us to make other shared tweaks to joblib.Parallel.
def CanalesParallel(n_jobs=4):
    return Parallel(n_jobs=n_jobs)


__all__ = [
    'CanalesParallel',
    'logger',
    'setup_environment',
    'TMC_CIVIS_DATABASE',
    'TMC_CIVIS_DATABASE_NAME',
    ]
