# Transform nested JSON array or object into new Db2 table
This example extracts nested elements from a JSON array and saves each array item into a Db2 table.  
**Note:** this is a basic example and does not include any primary key-foreign key relationships.

The example below will use this JSON document example:
```
{
  "_id": "90174e7b66274b00222ef6d8e60807f4",
  "_rev": "1-ff6661fc0e8d8496a1ad4868407ed91e",
  "array_of_objs": [
      "object0": {
        "number": 100,
        "boolean": true,
        "nestedKey": "value"
      },
      "object1": {
        "number": 200,
        "boolean": false,
        "nestedKey": "nextValue"
      }
  ]
}
```

## Python example
```python
# Import and initialize the SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Load Cloudant documents into a Spark DataFrame from the specified account
cloudantdata = spark.read.format("org.apache.bahir.cloudant")\
.option("cloudant.host", "account.cloudant.com")\
.option("cloudant.username", "username")\
.option("cloudant.password", "password")\
.load("database")

# Calculate the number of elements in the 'array_of_objs' column
nElements = len(cloudantdata.select('array_of_objs').collect()[0].array_of_objs)

# Create a new DataFrame for 'array_of_objs'
productList_array = cloudantdata.select('array_of_objs')

# Create properties for saving the DataFrame to a Db2 table
properties = {
    'user': 'username',
    'password': 'password',
    'driver': 'com.ibm.db2.jcc.DB2Driver'
}
db2_jdbc_url = 'jdbc:db2://****:50000/BLUDB'

# Create a new dataframe for each nested object in the 'array_of_objs' array
for i in range(nElements):
    # New dataframe for i item in 'array_of_objs' array
    new_table = productList_array.withColumn('array_of_objs-' + str(i), cloudantdata.productList.getItem(i))
    # Extract each nested object into their own column
    new_table = new_table.withColumn('array_of_objs-' + str(i) + '.number', new_table['array_of_objs-' + str(i)].number)\
    .withColumn('array_of_objs-' + str(i) + '.boolean', new_table['array_of_objs-' + str(i)].boolean)\
    .withColumn('array_of_objs-' + str(i) + '.nestedKey', new_table['array_of_objs-' + str(i)].nestedKey)    
    # Drop the 'array_of_objs' and 'array_of_objs-i' columns now that we have extracted all elements
    new_table = new_table.drop('array_of_objs').drop('array_of_objs-' + str(i))
    # Print the DataFrame
    new_table.show(5)
     # Save the DataFrame to a Db2 Warehouse 'array_of_objsi' table
    new_table.write.jdbc(db2_jdbc_url, 'array_of_objs' + str(i), properties=properties)
```
