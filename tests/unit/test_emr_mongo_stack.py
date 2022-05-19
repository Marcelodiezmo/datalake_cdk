import aws_cdk as core
import aws_cdk.assertions as assertions

from emr_mongo.emr_mongo_stack import EmrMongoStack

# example tests. To run these tests, uncomment this file along with the example
# resource in emr_mongo/emr_mongo_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EmrMongoStack(app, "emr-mongo")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
