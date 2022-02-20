from google.cloud import storage, pubsub_v1, securitycenter
from googleapiclient.discovery import build

def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the Asia region with the coldline storage class
    """
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="asia")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

def create_pubsub_topic(project_id, topic_id):
    """
    Create a pubsub topic queue for scc
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    topic = publisher.create_topic(request={"name": topic_path})

def create_scc_notifications(organization_id, project_name, notification_config_id, pubsub_topic):
    """
    Create security command center notifications
    """
    client = securitycenter.SecurityCenterClient()
    # TODO: organization_id = "your-org-id"
    # TODO: notification_config_id = "your-config-id"
    # TODO: pubsub_topic = "projects/{your-project-id}/topics/{your-topic-ic}"
    # Ensure this ServiceAccount has the "pubsub.topics.setIamPolicy" permission on the new topic.

    org_name = "organizations/{org_id}".format(org_id=organization_id)
    filter_name = "resource.project_display_name=" + "\"" + project_name + "\""
    created_notification_config = client.create_notification_config(
        request={
            "parent": org_name,
            "config_id": notification_config_id,
            "notification_config": {
                "description": "Notification for active findings for project " + project_name,
                "pubsub_topic": pubsub_topic,
                "streaming_config": {"filter": filter_name}
            },
        }
    )
    print(created_notification_config)

def create_dataflow(project_name, job, topic, bucketname):
    """
    Create dataflow job
    """
    template = 'gs://dataflow-templates-asia-southeast1/latest/Cloud_PubSub_to_GCS_Text'
    df_location="asia-southeast1"
    environment = {
       'bypassTempDirValidation': "false",
       'tempLocation': bucketname + "temp",
       'ipConfiguration': "WORKER_IP_UNSPECIFIED",
       'additionalExperiments': []
    }
    parameters = {
       'inputTopic': topic,
       'outputDirectory': bucketname,
       'outputFilenamePrefix': "output-",
       'outputFilenameSuffix': ".txt",
    }
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project_name,
        location=df_location,
        gcsPath=template,
        body={
            'jobName': job,
            'environment': environment,
            'parameters': parameters,
        }
    )
    response = request.execute()

def hello_pubsub(event, context):
    """
    Background Cloud Function to be triggered by Pub/Sub.
    """
    import base64
    import json

    print("""This Function was triggered by messageId {} published at {} to {}
    """.format(context.event_id, context.timestamp, context.resource["name"]))

    if 'data' in event:
        name = base64.b64decode(event['data']).decode('utf-8')
        value = json.loads(name)
        project_name = value['protoPayload']['request']['project']['projectId']
        project_number = value['protoPayload']['request']['project']['projectNumber']
        org_id = value['protoPayload']['request']['project']['parent']['id']
        scc_config_id = project_name + "-notification"
        create_bucket_class_location(project_name)
        create_pubsub_topic("jonchin-gps-argolis", project_name + "-topic")
        pubsub_topic = "projects/jonchin-gps-argolis/topics/" + project_name + "-topic"
        create_scc_notifications(org_id, project_name, scc_config_id, pubsub_topic)
        create_dataflow("jonchin-gps-argolis", project_name + "-dataflow", pubsub_topic, "gs://" + project_name + "/")
    else:
        name = 'Not data in pubsub event stream'
    print('Output {}!'.format(value))