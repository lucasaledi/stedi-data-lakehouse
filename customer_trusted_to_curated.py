import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1675265752183 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1675265752183",
)

# Script generated for node drop_duplicates
drop_duplicates_node1675265870793 = DynamicFrame.fromDF(
    customer_trusted_node1.toDF().dropDuplicates(["email"]),
    glueContext,
    "drop_duplicates_node1675265870793",
)

# Script generated for node Join
Join_node1675266057255 = Join.apply(
    frame1=accelerometer_landing_node1675265752183,
    frame2=drop_duplicates_node1675265870793,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1675266057255",
)

# Script generated for node filter_by_consent_date
SqlQuery0 = """
select * from myDataSource
where timestamp >= shareWithResearchAsOfDate
"""
filter_by_consent_date_node1675266106894 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Join_node1675266057255},
    transformation_ctx="filter_by_consent_date_node1675266106894",
)

# Script generated for node drop_fields
drop_fields_node1675266258210 = DropFields.apply(
    frame=filter_by_consent_date_node1675266106894,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="drop_fields_node1675266258210",
)

# Script generated for node customer_curated
customer_curated_node1675266410722 = glueContext.getSink(
    path="s3://lucasaledi-stedi/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1675266410722",
)
customer_curated_node1675266410722.setCatalogInfo(
    catalogDatabase="lucasaledi-stedi", catalogTableName="customer_curated"
)
customer_curated_node1675266410722.setFormat("json")
customer_curated_node1675266410722.writeFrame(drop_fields_node1675266258210)
job.commit()
