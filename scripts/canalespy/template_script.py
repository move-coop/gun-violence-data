# Civis Container Script: https://platform.civisanalytics.com/spa/#/scripts/containers/38223614
# ^ this is the template you can clone. Replace link above with new link.
# Write a couple of sentences on what this script does
import os
from parsons import Redshift, S3, Table #add any additional Parsons endpoints here
from canalespy import logger, setup_environment

def main():
    setup_environment()

    rs = Redshift()
    s3 = S3()

    # Any additional Civis parameters
    var_name = os.environ['PARAMETER_NAME']

    # INSERT ALL OF YOUR CODE HERE

    logger.info("The script has finished!") #this is a friendly reminder to add logging throughout :)

if __name__ == '__main__':
    main()
