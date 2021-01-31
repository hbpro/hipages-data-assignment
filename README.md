Design Choices and How to Run:

    0. Scala 2.12.13 has been selected, mostly as a personal preference.
    1. Apache Spark 3.1.0 is chosen as a scalable data processing engine.
    2. SystemParams object consists of values needed to run the job e.g. input path. To run the job, first edit the 
        input and output path to point to input json data and output directory.
    3. Input and output files will be written on the local filesystem, but you are welcome to change it to HDFS/S3/...
        in the SystemParams object.
    4. Spark master is set to local but in case of running the job on top of a Spark cluster e.g. YARN, master config
        should be passed as a Spark-Submit parameter.
    5. As the solution will be submitted using Spark-Submit, it is not containerized.
    6. To submit the job you need to package the solution using sbt.
    7. As there is no non-spark dependencies added, there is no need to pass dependency JARs as part of spark submit or
        make a FAT-JAR.
    8. Date field has been assumed in the "MM/dd/yyyy HH:mm:ss" format.
    9. Switching "org" and "akka" log off for the sake of simplicity specially during deployment.
    10. Spark ReadStream could have been used in order to read data from a message broker e.g. Apache Kafka, but for the
        sake of simplicity, batch processing is implemented.
    11. Malformed data are assumed to be dropped and in order to drop them, the option "mode", "DROPMALFORMED" is used
        when loading data to the platform.
    12. In order to make sure that the output file is just one, coalesce with 1 as parameter has been called which is
        obviously not a good choice during deployment as worker memory may explode as the input grows.
    13. To run the tests, you can simply run them using IDE or from the terminal, from project root by "sbt test" 
        command.
    14. sessionLength.csv has been added as an extra dataframe for the bonus question. It can be used to show the 
        session length for each user session. Which will be helpful in churn rate analysis and recomendation purposes.
    