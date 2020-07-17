import configparser
import boto3
import json

## FUNCTIONS:

def delete_cluster():
    """
    This function deletes the cluster we created to run this program, to avoid additional costs.
    """
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id = config.get('AWS','KEY'),
                       aws_secret_access_key = config.get('AWS','SECRET')
                       )
    
    try:
        redshift.delete_cluster( ClusterIdentifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER"),  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)

        
        
def delete_role():
    """
    This function deletes the role we created to run this program.
    """
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    iam = boto3.client('iam',aws_access_key_id = config.get('AWS','KEY'),
                     aws_secret_access_key = config.get('AWS','SECRET'),
                     region_name='us-west-2'
                  )

    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(e)
        
        
        
def reset_cluster_config():
    """
    This function resets the information in the config file related to the cluster and role we created.
    """
    config.set('CLUSTER','HOST', '')
    config.set('CLUSTER','DBNAME', '')
    config.set('CLUSTER','USER', '')
    config.set('CLUSTER','PASSWORD', '')
    config.set('CLUSTER','PORT', '')
    config.set('IAM_ROLE','ARN', '')

    with open('dwh.cfg','w') as configfile:
        config.write(configfile)

##SCRIPT:
# Open config parser

config = configparser.ConfigParser()
config.read('dwh.cfg')

delete_cluster()

delete_role()
    
reset_cluster_config()