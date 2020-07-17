import configparser
import boto3
import json
import time

## FUNCTIONS:

def cluster_starter ():
    """
    This function creates a Redshift cluster, with the configuration specified in the config file.
    """
    try:
        response = redshift.create_cluster(        
            # Parameters for hardware
            ClusterType = config.get("DWH","DWH_CLUSTER_TYPE"),
            NodeType = config.get("DWH","DWH_NODE_TYPE"),
            NumberOfNodes=int(config.get("DWH","DWH_NUM_NODES")),        

            # Parameters for identifiers & credentials
            DBName = config.get("DWH","DWH_DB"),
            ClusterIdentifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
            MasterUsername = config.get("DWH","DWH_DB_USER"),
            MasterUserPassword = config.get("DWH","DWH_DB_PASSWORD"),


            # TODO: add parameter for role (to allow s3 access)
            IamRoles=[iam.get_role(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"))['Role']['Arn']]
        )
    except Exception as e:
        print(e)

        
        
def role_creator ():
    """
    This function creates a role with the necessary permissions for the Redshift cluster to call S3 and other AWS services. This way, our cluster can extract our data which is stored in S3.
    """
        
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )

    except Exception as e:
        print(e)
    
    try:
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
    
    except Exception as e:
        print(e)
      
    
    
def wait_for_cluster_readiness():
    """
    This function waits for the cluster to be fully created (which is necessary to run the program) before continuing to run the program.
    """
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    try:
    
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        ClusterStatus = myClusterProps['ClusterStatus']

        while ClusterStatus != 'available':
            print ('Cluster is being created')
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            ClusterStatus = myClusterProps['ClusterStatus']
            time.sleep(60)
        print ('Cluster created')

        return myClusterProps

    except Exception as e:
        print(e)

    
    
def config_file_update(myClusterProps):
    """
    This function updates the config file with the information from the newly created role and cluster, for use by other parts of the program.
    """
    
    config.set('CLUSTER','HOST', myClusterProps['Endpoint']['Address'])
    config.set('IAM_ROLE','ARN',  myClusterProps['IamRoles'][0]['IamRoleArn'])
        
    config.set('CLUSTER','DBNAME', config.get("DWH","DWH_DB"))
    config.set('CLUSTER','USER', config.get("DWH","DWH_DB_USER"))
    config.set('CLUSTER','PASSWORD', config.get("DWH","DWH_DB_PASSWORD"))
    config.set('CLUSTER','PORT', config.get("DWH","DWH_PORT"))


    with open('dwh.cfg','w') as configfile:
        config.write(configfile) 
        
        
        
def open_tcp_port():
    """
    This function opens a TCP port to access the DWH.
    """
    DWH_PORT = config.get("DWH","DWH_PORT")
    
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id = config.get('AWS','KEY'),
                       aws_secret_access_key = config.get('AWS','SECRET')
                        )

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

## SCRIPT:
# Open config parser:

config = configparser.ConfigParser()
config.read('dwh.cfg')

# Role creation and policy attachment:

try:
        
    iam = boto3.client(
                'iam',aws_access_key_id = config.get('AWS','KEY'),
                aws_secret_access_key = config.get('AWS','SECRET'),
                region_name='us-west-2'
                )

except Exception as e:
    print(e)
    
role_creator()

# Cluster creation:

try:
        
    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id = config.get('AWS','KEY'),
                           aws_secret_access_key = config.get('AWS','SECRET')
                           )

except Exception as e:
    print(e)

cluster_starter()

myClusterProps = wait_for_cluster_readiness()

# Update config file with config info from newly created Redshift cluster

config_file_update(myClusterProps)

# Open a TCP port
    
open_tcp_port()

