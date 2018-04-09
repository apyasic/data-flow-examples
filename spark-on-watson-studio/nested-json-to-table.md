# Transform nested JSON array or object into new Db2 table
This example extracts nested elements from a JSON array named 'productList' and saves each array item into a Db2 table.  
**Note:** this is a basic example and does not include any primary key-foreign key relationships.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

cloudantdata = spark.read.format("org.apache.bahir.cloudant")\
.option("cloudant.host", "account.cloudant.com")\
.option("cloudant.username", "username")\
.option("cloudant.password", "password")\
.load("database")

# Calculate the number of elements in the 'productList' column
nElements = len(cloudantdata.select('productList').collect()[0].productList)

# Create new dataframe from 'productList' array
productList_array = cloudantdata.select('productList')

# Properties for saving to Db2 table
properties = {
    'user': 'username',
    'password': 'password',
    'driver': 'com.ibm.db2.jcc.DB2Driver'
}

db2_jdbc_url = 'jdbc:db2://****:50000/BLUDB'

# Create a new dataframe for each nested object in the 'productList' array
for i in range(nElements):
    # New dataframe for i item in 'productList' array
    new_table = productList_array.withColumn('productList-' + str(i), cloudantdata.productList.getItem(i))
    # Extract each nested object into their own column
    new_table = new_table.withColumn('productList-' + str(i) + '.Ingrediants', new_table['productList-' + str(i)].Ingrediants)    .withColumn('productList-' + str(i) + '.brandName', new_table['productList-' + str(i)].brandName)    .withColumn('productList-' + str(i) + '.company', new_table['productList-' + str(i)].Ingrediants)    .withColumn('productList-' + str(i) + '.registrationNumber', new_table['productList-' + str(i)].registrationNumber)
    # Drop the 'productList' and 'productList-i' columns now that we have extracted all elements
    new_table = new_table.drop('productList').drop('productList-' + str(i))
    # Print the DataFrame
    new_table.show(5)
     # Save the DataFrame to a Db2 Warehouse 'productListTablei' table
    new_table.write.jdbc(db2_jdbc_url, 'productList' + str(i), properties=properties)
```
