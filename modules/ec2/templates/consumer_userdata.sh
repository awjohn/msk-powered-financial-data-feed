#!/bin/bash
yum update -y
yum install -y java-1.8.0-openjdk-devel python3 pip git

# Install Kafka
KAFKA_VERSION="2.8.1"
SCALA_VERSION="2.13"
wget "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka

# Install AWS CLI
pip3 install awscli

# Clone the project repository
cd /home/ec2-user
git clone https://github.com/aws-samples/amazon-msk-java-app-samples.git
chown -R ec2-user:ec2-user amazon-msk-java-app-samples

# Set environment variables
echo "export PATH=\$PATH:/opt/kafka/bin" >> /home/ec2-user/.bashrc
echo "export MSK_SECURITY_GROUP=${msk_security_group}" >> /home/ec2-user/.bashrc

# Copy consumer script
cat > /home/ec2-user/run-consumer.sh << 'EOF'
#!/bin/bash
# Get MSK cluster information
CLUSTER_ARN=$(aws kafka list-clusters --query 'ClusterInfoList[0].ClusterArn' --output text)
ZOOKEEPER=$(aws kafka describe-cluster --cluster-arn $CLUSTER_ARN --query 'ClusterInfo.ZookeeperConnectString' --output text)
BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn $CLUSTER_ARN --query 'BootstrapBrokerString' --output text)

# Run the consumer
cd /home/ec2-user/amazon-msk-java-app-samples
mvn clean package
java -jar target/amazon-msk-samples-1.0-SNAPSHOT.jar consumer $BROKERS
EOF

chmod +x /home/ec2-user/run-consumer.sh
chown ec2-user:ec2-user /home/ec2-user/run-consumer.sh
