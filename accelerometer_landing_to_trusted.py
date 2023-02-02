import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame


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
accelerometer_landing_node1675076059389 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1675076059389",
)

# Script generated for node join_customer_accelerometer_landings
accelerometer_landing_node1675076059389DF = (
    accelerometer_landing_node1675076059389.toDF()
)
customer_trusted_node1DF = customer_trusted_node1.toDF()
join_customer_accelerometer_landings_node2 = DynamicFrame.fromDF(
    accelerometer_landing_node1675076059389DF.join(
        customer_trusted_node1DF,
        (
            accelerometer_landing_node1675076059389DF["user"]
            == customer_trusted_node1DF["email"]
        ),
        "left",
    ),
    glueContext,
    "join_customer_accelerometer_landings_node2",
)

# Script generated for node filter_by_consent_date
SqlQuery0 = """
select * from myDataSource
where timestamp >= shareWithResearchAsOfDate
"""
filter_by_consent_date_node1675265287289 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": join_customer_accelerometer_landings_node2},
    transformation_ctx="filter_by_consent_date_node1675265287289",
)

# Script generated for node drop_fields
drop_fields_node1675076288681 = DropFields.apply(
    frame=filter_by_consent_date_node1675265287289,
    paths=[
        "email",
        "customerName",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="drop_fields_node1675076288681",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node3 = glueContext.getSink(
    path="s3://lucasaledi-stedi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node3",
)
accelerometer_trusted_node3.setCatalogInfo(
    catalogDatabase="lucasaledi-stedi", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node3.setFormat("json")
accelerometer_trusted_node3.writeFrame(drop_fields_node1675076288681)
job.commit()
