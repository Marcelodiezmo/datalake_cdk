import json
import boto3
import os

def main(event, context):
    client = boto3.client('emr')
    # TODO implement
    res=client.run_job_flow(Name='Datalake',LogUri='s3://'+os.environ['bucket_log']+'/HDFS/',ReleaseLabel='emr-6.2.0',
    Applications=[
        {
            'Name': 'Spark'
        },
        {
            'Name': 'JupyterHub'
        },
        {
            'Name': 'Livy'
        },
        {
            'Name': 'Ganglia'
        },
        {
            'Name': 'Hue'
        },
        {
            'Name': 'Hive'
        },
        {
            'Name': 'TensorFlow'
        },
        {
            'Name': 'HBase'
        }
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'r4.xlarge',
                'InstanceCount': 1,
                "Configurations":[
                {
                 "Classification": "spark-defaults",
                 "Properties": {
                   "spark.dynamicAllocation.enabled": "true",
                   "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                   "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                   "spark.storage.level": "MEMORY_AND_DISK_SER",
                   "spark.rdd.compress": "true",
                   "spark.shuffle.compress": "true",
                   "spark.shuffle.spill.compress": "true",
                 }
                }
                
            ]
            },
            {
                'Name': "Slave",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'd2.xlarge',
                'InstanceCount': 1,
                "Configurations":[
                {
                 "Classification": "spark-defaults",
                 "Properties": {
                   "spark.dynamicAllocation.enabled": "tr",
                   "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                   "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                   "spark.storage.level": "MEMORY_AND_DISK_SER",
                   "spark.rdd.compress": "true",
                   "spark.shuffle.compress": "true",
                   "spark.shuffle.spill.compress": "true",
                 }
                }
            ]
            }
        ],
        'Ec2KeyName': os.environ['keys'],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': os.environ['subnet'],
    },
    Steps=[
    #
        {
            'Name': 'git',   
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': ['s3://'+os.environ['bucket_name']+'/git/skynetcode/'+os.environ['prefix']+'/emr_git_'+os.environ['branch']+'.sh']
                    }
        
        },
        {
            'Name': 'skynetModule'+ os.environ['branch'],   
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': ['s3://'+os.environ['bucket_name']+'/git/skynetcode/'+os.environ['prefix']+'/ml_prepare.sh']
                    }
        
        },
        {
            "Name": "in_py_compareqasetup.py",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep":{
                        "Jar": "s3://elasticmapreduce/libs/script-runner/script-runner.jar", 
                        "Args": ["s3://"+os.environ['bucket_name']+"/git/skynetcode/"+os.environ['prefix']+"/pipeline/config/in_run_compareqasetup.sh"]
                    }
            
        },
        {
            "Name": "in_pys_compareqasetup.py",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep":{"Args": [
                                    "spark-submit",
                                    "--deploy-mode",
                                    "cluster",
                                    "s3://"+os.environ['bucket_name']+"/git/skynetcode/"+os.environ['prefix']+"/pipeline/scripts/in_pys_compareqasetup.py"
                                    ],
                                    "Jar": "command-runner.jar"}
        },
        {
            "Name": "in_py_compareqasetupxlsx.py",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep":{
                        "Jar": "s3://elasticmapreduce/libs/script-runner/script-runner.jar" ,
                        "Args": ["s3://"+os.environ['bucket_name']+"/git/skynetcode/"+os.environ['prefix']+"/pipeline/config/in_run_compareqasetupxlsx.sh"],
                    }
        },
        {
            "Name": "in_py_compareqasetupcom.py",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep":{
                        "Jar": "s3://elasticmapreduce/libs/script-runner/script-runner.jar", 
                        "Args": ["s3://"+os.environ['bucket_name']+"/git/skynetcode/"+os.environ['prefix']+"/pipeline/config/in_run_compareqasetupcom.sh"]
                    }
            
        },
        {
            "Name": "in_pys_compareqasetupcom.py",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep":{"Args": [
                                    "spark-submit",
                                    "--deploy-mode",
                                    "cluster",
                                    "s3://"+os.environ['bucket_name']+"/git/skynetcode/"+os.environ['prefix']+"/pipeline/scripts/in_pys_compareqasetupcom.py"
                                    ],
                                    "Jar": "command-runner.jar"}
        },
        {
            "Name": "in_py_compareqasetupcomxlsx.py",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep":{
                        "Jar": "s3://elasticmapreduce/libs/script-runner/script-runner.jar" ,
                        "Args": ["s3://"+os.environ['bucket_name']+"/git/skynetcode/"+os.environ['prefix']+"/pipeline/config/in_run_compareqasetupcomxlsx.sh"],
                    }
        },
        {
            'Name': 'vimeoData'+ os.environ['branch'],   
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': ['s3://'+os.environ['bucket_name']+'/git/skynetcode/'+os.environ['prefix']+'/pipeline/config/in_vimeodata.sh']
                    }
        
        },
        {
            'Name': 'zoomData'+ os.environ['branch'],   
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': ['s3://'+os.environ['bucket_name']+'/git/skynetcode/'+os.environ['prefix']+'/pipeline/config/in_zoomdata.sh']
                    }
        
        },
           {
            'Name': 'hubspotData'+ os.environ['branch'],   
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': ['s3://'+os.environ['bucket_name']+'/git/skynetcode/'+os.environ['prefix']+'/pipeline/config/in_hubspotdata.sh']
                    }
           },
            {
            'Name': 'readListUsersProgressMicrocredentials'+ os.environ['branch'],   
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': ['s3://'+os.environ['bucket_name']+'/git/skynetcode/'+os.environ['prefix']+'/pipeline/config/in_py_readlistusersprogressdrive.sh']
                    }
           },
                   {
            "Name": "progressubitsMicrocredentials.py",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep":{"Args": [
                                    "spark-submit",
                                    "--deploy-mode",
                                    "cluster",
                                    "s3://"+os.environ['bucket_name']+"/git/skynetcode/"+os.environ['prefix']+"/pipeline/scripts/in_pys_progressubits.py"
                                    ],
                                    "Jar": "command-runner.jar"}
        },
            {
            'Name': 'writeUsersProgressMicrocredentials'+ os.environ['branch'],   
                    'ActionOnFailure':'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': ['s3://'+os.environ['bucket_name']+'/git/skynetcode/'+os.environ['prefix']+'/pipeline/config/in_py_writeusersprogressdrive.sh']
                    }
           }
       
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION')
    return {
        'statusCode': 200,
        'body': json.dumps(res['JobFlowId'])
    }
#