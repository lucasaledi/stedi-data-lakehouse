""" STEP_TRAINER_TRUSTED STORES EMAIL DATA TO BE USED FOR JOINING WITH \
    ACCELEROMETER_TRUSTED TABLE
"""
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

# Script generated for node customer_curated
customer_curated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1675334617895 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1675334617895",
)

# Script generated for node rename_serialNumber
rename_serialNumber_node1675334842500 = RenameField.apply(
    frame=step_trainer_landing_node1675334617895,
    old_name="serialNumber",
    new_name="st_serialNumber",
    transformation_ctx="rename_serialNumber_node1675334842500",
)

# Script generated for node join_tables
join_tables_node1675334885143 = Join.apply(
    frame1=rename_serialNumber_node1675334842500,
    frame2=customer_curated_node1,
    keys1=["st_serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="join_tables_node1675334885143",
)

# Script generated for node drop_fields
drop_fields_node1675334928655 = DropFields.apply(
    frame=join_tables_node1675334885143,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "phone",
        "st_serialNumber",
    ],
    transformation_ctx="drop_fields_node1675334928655",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1675335296137 = glueContext.getSink(
    path="s3://lucasaledi-stedi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1675335296137",
)
step_trainer_trusted_node1675335296137.setCatalogInfo(
    catalogDatabase="lucasaledi-stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1675335296137.setFormat("json")
step_trainer_trusted_node1675335296137.writeFrame(drop_fields_node1675334928655)
job.commit()