import json
from datetime import date
import sys
import logging
import psycopg2
import boto3
import botocore
from datetime import datetime
from botocore.exceptions import ClientError
import base64

# rds settings
rds_host_pub = "rds-host-siteA"
rds_host_sub = "rds-host-siteB"
db_user = "postgres"
db_name_pub = "pub10"
db_name_sub = "sub10"
db_port = "5432"
lag = 0
#lag threshold in seconds
lag_threshold = 300
is_bi_rep = "true"
reg_name = "us-east-1"
insight_id = "1234"
param_name = f"{insight_id}-pglogical-avoid-dup-alert-heartbeat-"
param_name_source = param_name + db_name_pub
param_name_target = param_name + db_name_sub
MY_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:060725138335:1234-test-lab-us-east-1-pg-project_topic'
secret_name_pub = "pub-secrets"
secret_name_sub = "sub-secrets"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(s):
    secret_name = s
    region_name = reg_name
    # Create a Secrets Manager client
    sm_session = boto3.session.Session()
    sm_client = sm_session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            # decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(secret)

def connect_db(loc,d_n,d_u,sec,r_h):
    pwd_s = get_secret(sec)
    pwd = pwd_s['password']
    try:
        conn_db = psycopg2.connect(dbname=d_n, user=d_u, password=pwd, host=r_h)
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: Could not connect to {loc} PostgreSql instance.")
        logger.error(e)
        send_alert(e,0,r_h,d_n,'','')
        sys.exit()
    logger.info(f"SUCCESS: Connection to RDS {loc} PostgreSql instance succeeded")
    return conn_db

def check_lag(sour_date,rep_date):
    sour_date_con = sour_date.replace(second=0, microsecond=0)
    rep_date_str = str(rep_date)
    logger.info(f'Source heart beat date and time is {sour_date_con}')
    # If subscriber not replicated first time, the heartbeat date is null/empty. So consider to send alert in this case.
    if not rep_date_str.strip():
        logger.info(f'Setting lag to default {lag_threshold} as there is no data in target site heartbeat table')
        l = lag_threshold
    else:
        rep_date_con = rep_date.replace(second=0, microsecond=0)
        logger.info(f'Target heart beat date and time is {rep_date_con}')
        l = abs((sour_date_con - rep_date_con).total_seconds())
    logger.info(f'Lag for Source -> Target is {l}')
    return l

def del_param(p,r_h,d):
    ssm = boto3.client('ssm')
    try:
        ssm.delete_parameter(
            Name=p
            )
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: Could not delete parameter!")
        logger.error(e)
        send_alert(e,0,r_h,d,'','')
        sys.exit()

def put_param(p,cd,rs,r_h,d):
    ssm = boto3.client('ssm')
    try:
        ssm.put_parameter(
            Name=p,
            Value=f'{cd},{rs}',
            Type='StringList',
            Overwrite=True
        )
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: Could not put parameter!")
        logger.error(e)
        send_alert(e,0,r_h,d,'','')
        sys.exit()
            
def send_alert(r_status,lags,rds_host,s_db_name,t_db_name,ssm_param_name):
    sns_client = boto3.client('sns')
    ssm = boto3.client('ssm')
    cur_date = str(date.today())
    if r_status == 'solved':
        msg1=f'Instance: {rds_host} has {str(lags)} seconds lag.'
        msg2=f'DBName replication direction: {s_db_name} -> {t_db_name}'
        sub=f'Monitoring pglogical replication lag resolved for {s_db_name} -> {t_db_name}'
    elif r_status == 'not_solved':
        msg1=f'Instance: {rds_host} has {str(lags)} seconds lag.'
        msg2=f'DBName replication direction: {s_db_name} -> {t_db_name}'
        sub=f'Monitoring pglogical detected replication lag for {s_db_name} -> {t_db_name}'
    else:
        msg1=f'Instance: {rds_host} has an exception for dbname {s_db_name}, exiting now!'
        msg2=f'Exception: {r_status}'
        sub=f'Monitoring pglogical has an exception for {s_db_name}'

    #Publish alert
    try:
        sns_client.publish(
            TopicArn=MY_SNS_TOPIC_ARN,
            Message=msg1 + ' \n' +
                    msg2,
            Subject=sub
            )
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: Could not publish alert!")
        logger.error(e)
        sys.exit()

    if r_status == 'solved' or r_status == 'not_solved':
        #delete existing versions
        del_param(ssm_param_name,rds_host,s_db_name)
        #Put parameter
        put_param(ssm_param_name,cur_date,r_status,rds_host,s_db_name)

def get_param_value(p_n,r_h,d_n):
    ssm = boto3.client('ssm')
    try:
        param = ssm.get_parameter(Name=p_n, WithDecryption=True)
    except ssm.exceptions.ParameterNotFound:
        # Check if this parameter exist in param store. If not create one with dummy value.
        put_param(p_n,'1999-01-01','not_solved',r_h,d_n)
    try:
        param = ssm.get_parameter(Name=p_n, WithDecryption=True)
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: Could not put parameter!")
        send_alert(e,0,r_h,d_n,'','')
        logger.error(e)
        sys.exit()
    param_value_l = param['Parameter']['Value']
    logger.info(f'param value list is {param_value_l}')
    param_value_l = param_value_l.split(',')
    logger.info(f'param value list is {param_value_l}')
    param_value = param_value_l[0]
    rep_status = param_value_l[1]
    logger.info(f'param_value is {param_value}')
    logger.info(f'rep_status is {rep_status}')
    return param_value,rep_status


def update_db(site,db_name,db_u,d_sec,rds_h):
    conn = connect_db(site,db_name,db_u,d_sec,rds_h)
    try:
        cur = conn.cursor()
        # Update the heartbeat table at {site} side with current date and time.
        update_date = datetime.now()
        logger.info(f'Updating the heartbeat table at {site} side with current date and time as {update_date}')
        cur.execute("Update pglogical_heartbeat_monitor set last_updated=%s WHERE dbname=%s", [update_date, db_name])
        conn.commit()
    except psycopg2.InterfaceError:
        pass

def lambda_handler(event, context):
    """
    This function checks and alerts uni direction and bidirectional replication lag in postgres pglogical setup. 
    Author: Vijay Reddy G
    """

    #Connect to source
    conn_pub = connect_db("source",db_name_pub,db_user,secret_name_pub,rds_host_pub)
    sub_date_bi = ''
    pub_date = ''
    try:
        cur_p = conn_pub.cursor()
        cur_p.execute("SELECT dbname,last_updated FROM pglogical_heartbeat_monitor")
        for row in cur_p:
            logger.info(row)
            if row[0] == db_name_pub:
                pub_date = row[1]
            if row[0] == db_name_sub:
                if row[1] is not None:
                    sub_date_bi = row[1]
    except psycopg2.InterfaceError:
        pass
    logger.info(f'Source date and time is {pub_date}')
    logger.info(f'Source date and time for bidirectional is {sub_date_bi}')

    #Connect to target
    conn_sub = connect_db("target", db_name_sub, db_user, secret_name_sub, rds_host_sub)
    pub_date_bi = ''
    sub_date = ''
    try:
        cur_sub = conn_sub.cursor()
        cur_sub.execute("SELECT dbname,last_updated FROM pglogical_heartbeat_monitor")
        for row in cur_sub:
            logger.info(row)
            if row[0] == db_name_pub:
                if row[1] is not None:
                    pub_date_bi = row[1]
            if row[0] == db_name_sub:
                sub_date = row[1]
    except psycopg2.InterfaceError:
        pass
    logger.info(f'Target date and time is {pub_date_bi}')
    logger.info(f'Target date and time for bidirectional is {sub_date}')

    ssm_param_value_source,reps_status = get_param_value(param_name_source,rds_host_pub,db_name_pub)
    if is_bi_rep == "true":
        ssm_param_value_target,rept_status = get_param_value(param_name_target,rds_host_sub,db_name_sub)
    else:
        ssm_param_value_target = ''
        rept_status = ''
    current_date = str(date.today())

    #Check lag and send alert for source->target
    lag_s_t = check_lag(pub_date, pub_date_bi)
    if ((ssm_param_value_source != current_date) and (lag_s_t >= lag_threshold)) or ((ssm_param_value_source == current_date) and (lag_s_t >= lag_threshold) and (reps_status == 'solved')):
        logger.info(f'Sending alert for {db_name_pub} -> {db_name_sub}')
        send_alert('not_solved',lag_s_t,rds_host_pub,db_name_pub,db_name_sub,param_name_source)
    elif (ssm_param_value_source == current_date) and (reps_status == 'not_solved') and (lag_s_t < lag_threshold):
        logger.info('Sending alert as replication resolved for source -> target')
        send_alert('solved',lag_s_t,rds_host_pub,db_name_pub,db_name_sub,param_name_source)
    else:
        logger.info(f'Not sending alert, either alert is already sent for today or no lag observed for {db_name_pub} -> {db_name_sub}!!')

    #check lag and send alert for target->source
    if is_bi_rep == "true":
        # Check lag for target->source
        lag_t_s = check_lag(sub_date, sub_date_bi)
        if ((ssm_param_value_target != current_date) and (lag_t_s >= lag_threshold)) or ((ssm_param_value_target == current_date) and (lag_t_s >= lag_threshold) and (rept_status == 'solved')):
            logger.info(f'Sending alert for {db_name_sub} -> {db_name_pub}')
            send_alert('not_solved',lag_t_s,rds_host_sub,db_name_sub,db_name_pub,param_name_target)
        elif (ssm_param_value_target == current_date) and (rept_status == 'not_solved') and (lag_t_s < lag_threshold):
            logger.info('Sending alert as replication resolved for target -> source')
            send_alert('solved',lag_t_s,rds_host_sub,db_name_sub,db_name_pub,param_name_target)
        else:
            logger.info(f'Not sending alert, either alert is already sent for today or no lag observed for bidirectional {db_name_sub} -> {db_name_pub}!!')
  
    # update db with latest date at source
    update_db("source",db_name_pub,db_user,secret_name_pub,rds_host_pub)

    # update db with latest date at target
    if is_bi_rep == "true":
        update_db("target",db_name_sub,db_user,secret_name_sub,rds_host_sub)






