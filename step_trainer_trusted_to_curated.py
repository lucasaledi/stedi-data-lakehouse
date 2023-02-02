import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1675338261695 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1675338261695",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1675338264071 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1675338264071",
)

# Script generated for node Join
Join_node1675338371015 = Join.apply(
    frame1=accelerometer_trusted_node1675338261695,
    frame2=step_trainer_trusted_node1675338264071,
    keys1=["user", "timeStamp"],
    keys2=["email", "sensorReadingTime"],
    transformation_ctx="Join_node1675338371015",
)

# Script generated for node step_trainer_curated
step_trainer_curated_node1675338434403 = glueContext.getSink(
    path="s3://lucasaledi-stedi/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_curated_node1675338434403",
)
step_trainer_curated_node1675338434403.setCatalogInfo(
    catalogDatabase="lucasaledi-stedi", catalogTableName="machine_learning_curated"
)
step_trainer_curated_node1675338434403.setFormat("json")
step_trainer_curated_node1675338434403.writeFrame(Join_node1675338371015)
job.commit()
