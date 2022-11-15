from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

client = boto3.client(
    'emr', region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': 'JVMG',
    'start_date': datetime(2022, 4, 2)
}

@dag(default_args=default_args, schedule_interval="@once", description="Job Spark no EMR", catchup=False, tags=['Spark','EMR'])
def trabalho_final():

    inicio = EmptyOperator(task_id='inicio')

    @task
    def tarefa_inicial():
        print("Início")

    @task
    def emr_create_cluster():
        cluster_id = client.run_job_flow(
            Name='jvmg_airflow',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://aws-logs-002112264889-us-east-1/elasticmapreduce/',
            ReleaseLabel='emr-6.8.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-0701a755ea796fd65'
            },
            Applications=[{'Name': 'Spark'}, {'Name': 'Hive'}]
        )
        return cluster_id["JobFlowId"]

    @task
    def wait_emr_cluster(cid: str):
        waiter = client.get_waiter('cluster_running')
        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )
        return True

    @task
    def emr_process_votos(cid: str):
        newstep = client.add_job_flow_steps(
            JobFlowId=cid,
            Steps=[
                {
                    'Name': 'Processa Indicadores dos Votos',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                '--packages', 'io.delta:delta-core_2.12:2.1.0',
                                's3://emr-code-002112264889/jvmg/pyspark/votos_codigo.py'
                                ]
                    }
                }
            ]
        )
        return newstep['StepIds'][0]

    @task
    def wait_emr_job(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')
        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 600
            }
        )

    @task
    def terminate_emr_cluster(cid: str):
        res = client.terminate_job_flows(
            JobFlowIds=[cid]
        )

    fim = EmptyOperator(task_id="fim")

    # Orquestração
    tarefainicial = tarefa_inicial()
    cluster = emr_create_cluster()

    inicio >> tarefainicial >> cluster

    esperacluster = wait_emr_cluster(cluster)
    indicadores = emr_process_votos(cluster) 

    esperacluster >> indicadores

    wait_step = wait_emr_job(cluster, indicadores)
    terminacluster = terminate_emr_cluster(cluster)

    wait_step >> terminacluster >> fim

execucao = trabalho_final()
