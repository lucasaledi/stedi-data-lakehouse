import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node customer_landing
customer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lucasaledi-stedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1",
)

# Script generated for node filter_by_consent
SqlQuery0 = """
select * from myDataSource
where sharewithresearchasofdate is not NULL
"""
filter_by_consent_node1675264862928 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": customer_landing_node1},
    transformation_ctx="filter_by_consent_node1675264862928",
)

# Script generated for node customer_trusted
customer_trusted_node3 = glueContext.getSink(
    path="s3://lucasaledi-stedi/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node3",
)
customer_trusted_node3.setCatalogInfo(
    catalogDatabase="lucasaledi-stedi", catalogTableName="customer_trusted"
)
customer_trusted_node3.setFormat("json")
customer_trusted_node3.writeFrame(filter_by_consent_node1675264862928)
job.commit()
