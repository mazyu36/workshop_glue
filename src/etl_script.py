import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
import re


# Script generated for node Remove Records with NULL
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF().na.drop()
    results = DynamicFrame.fromDF(df, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Yellow Trip Data
YellowTripData_node1712932670785 = glueContext.create_dynamic_frame.from_catalog(
    database="nyctaxi_db",
    table_name="raw_yellow_tripdata",
    transformation_ctx="YellowTripData_node1712932670785",
)

# Script generated for node Dropoff Zone Lookup
DropoffZoneLookup_node1712933650618 = glueContext.create_dynamic_frame.from_catalog(
    database="nyctaxi_db",
    table_name="taxi_zone_lookup",
    transformation_ctx="DropoffZoneLookup_node1712933650618",
)

# Script generated for node Pickup Zone Lookup
PickupZoneLookup_node1712932984582 = glueContext.create_dynamic_frame.from_catalog(
    database="nyctaxi_db",
    table_name="taxi_zone_lookup",
    transformation_ctx="PickupZoneLookup_node1712932984582",
)

# Script generated for node Remove Records with NULL
RemoveRecordswithNULL_node1712932811741 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"YellowTripData_node1712932670785": YellowTripData_node1712932670785},
        glueContext,
    ),
)

# Script generated for node ApplyMapping - Dropoff Zone Lookup
ApplyMappingDropoffZoneLookup_node1712933702271 = ApplyMapping.apply(
    frame=DropoffZoneLookup_node1712933650618,
    mappings=[
        ("locationid", "long", "do_location_id", "long"),
        ("borough", "string", "do_borough", "string"),
        ("zone", "string", "do_zone", "string"),
        ("service_zone", "string", "do_service_zone", "string"),
    ],
    transformation_ctx="ApplyMappingDropoffZoneLookup_node1712933702271",
)

# Script generated for node ApplyMapping - Pickup Zone Lookup
ApplyMappingPickupZoneLookup_node1712933302024 = ApplyMapping.apply(
    frame=PickupZoneLookup_node1712932984582,
    mappings=[
        ("locationid", "long", "pu_location_id", "long"),
        ("borough", "string", "pu_borough", "string"),
        ("zone", "string", "pu_zone", "string"),
        ("service_zone", "string", "pu_service_zone", "string"),
    ],
    transformation_ctx="ApplyMappingPickupZoneLookup_node1712933302024",
)

# Script generated for node SelectFromCollection
SelectFromCollection_node1712932868838 = SelectFromCollection.apply(
    dfc=RemoveRecordswithNULL_node1712932811741,
    key=list(RemoveRecordswithNULL_node1712932811741.keys())[0],
    transformation_ctx="SelectFromCollection_node1712932868838",
)

# Script generated for node Filter - Yellow Trip Data
FilterYellowTripData_node1712932894260 = Filter.apply(
    frame=SelectFromCollection_node1712932868838,
    f=lambda row: (bool(re.match("^2020-01", row["tpep_pickup_datetime"]))),
    transformation_ctx="FilterYellowTripData_node1712932894260",
)

# Script generated for node Join
Join_node1712933375840 = Join.apply(
    frame1=FilterYellowTripData_node1712932894260,
    frame2=ApplyMappingPickupZoneLookup_node1712933302024,
    keys1=["pulocationid"],
    keys2=["pu_location_id"],
    transformation_ctx="Join_node1712933375840",
)

# Script generated for node Join
Join_node1712933747452 = Join.apply(
    frame1=ApplyMappingDropoffZoneLookup_node1712933702271,
    frame2=Join_node1712933375840,
    keys1=["do_location_id"],
    keys2=["dolocationid"],
    transformation_ctx="Join_node1712933747452",
)

# Script generated for node ApplyMapping - Joined Data
ApplyMappingJoinedData_node1713143063541 = ApplyMapping.apply(
    frame=Join_node1712933747452,
    mappings=[
        ("do_location_id", "long", "do_location_id", "long"),
        ("do_borough", "string", "do_borough", "string"),
        ("do_zone", "string", "do_zone", "string"),
        ("do_service_zone", "string", "do_service_zone", "string"),
        ("vendorid", "bigint", "vendor_id", "long"),
        ("tpep_pickup_datetime", "string", "pickup_datetime", "timestamp"),
        ("tpep_dropoff_datetime", "string", "dropoff_datetime", "timestamp"),
        ("passenger_count", "bigint", "passenger_count", "long"),
        ("trip_distance", "double", "trip_distance", "double"),
        ("ratecodeid", "bigint", "ratecodeid", "long"),
        ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"),
        ("payment_type", "bigint", "payment_type", "long"),
        ("fare_amount", "double", "fare_amount", "double"),
        ("extra", "double", "extra", "double"),
        ("mta_tax", "double", "mta_tax", "double"),
        ("tip_amount", "double", "tip_amount", "double"),
        ("tolls_amount", "double", "tolls_amount", "double"),
        ("improvement_surcharge", "double", "improvement_surcharge", "double"),
        ("total_amount", "double", "total_amount", "double"),
        ("congestion_surcharge", "double", "congestion_surcharge", "double"),
        ("pu_location_id", "long", "pu_location_id", "long"),
        ("pu_borough", "string", "pu_borough", "string"),
        ("pu_zone", "string", "pu_zone", "string"),
        ("pu_service_zone", "string", "pu_service_zone", "string"),
    ],
    transformation_ctx="ApplyMappingJoinedData_node1713143063541",
)

# Script generated for node Transformed Yellow Trip Data
TransformedYellowTripData_node1712933832721 = (
    glueContext.write_dynamic_frame.from_options(
        frame=ApplyMappingJoinedData_node1713143063541,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": f"s3://{args['BUCKET_NAME']}/nyc-taxi/yellow-tripdata/",
            "partitionKeys": [],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="TransformedYellowTripData_node1712933832721",
    )
)

job.commit()
