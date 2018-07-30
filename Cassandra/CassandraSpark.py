from pyspark.sql import SparkSession, Row, functions

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

if __name__ == "__main__":
    # Create Spark Session
    spark = SparkSession.builder.appName("CassandraIntegration")\
            .config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
    
    # Get raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")

    users = lines.map(parseInput)

    usersDataset = spark.createDataFrame(users)

    # Write to Cassandra
    usersDataset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="users", keyspace="movielens")\
        .save()

    # React it back from Cassandra into a new DataFrame
    readUsers = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table="users", keyspace="movielens")\
        .load()

    readUsers.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show()

    spark.stop()
