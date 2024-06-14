# Hadoop Filestorage Class for Python, Sample app using Flask

An interface for Apache Hadoop File Storage using python, and a sample app in Flask using it to store files on a Secure Apache Hadoop Cluster

## Instructions

1. Install requirements.txt in your environment
2. Set the following Environment Variables
   ```terminal.sh
   export HADOOP_USER=your_hadoop_user
   export HADOOP_HOST=your_hadoop_host
   export HADOOP_PORT=your_hadoop_port
   export HADOOP_SECURE=1 # If your cluster uses Kerberos Authentication.
   ```
3. Initialize the DB
   ```flask --app flaskhadoop init-db```
4. Run the flask app
   ```flask --app flaskhadoop run```


## Hadoop Cluster Setup

For testing purposes, you can set up a single node Hadoop pseudo-cluster. Follow the instructions provided [here](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html).

Instructions to setup a Fully Distributed Cluster can be found [here](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html).

Secure access has been implemented through Kerberos Authentication - [Secure Hadoop Cluster Setup](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html)

## Notes

- If using Kerberos Authentication, the user running the Flask app must be authenticated as per the requirements of your Hadoop Cluster.
