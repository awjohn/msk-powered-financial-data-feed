aws_region = "us-east-1"

cluster_name    = "kafka-streaming-cluster"
kafka_version   = "2.8.1"
broker_nodes    = 3
instance_type   = "kafka.t3.small"
ebs_volume_size = 100

vpc_name = "kafka-streaming-vpc"
vpc_cidr = "10.0.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

ec2_instance_type = "t3.medium"
producer_instance_count = 1
consumer_instance_count = 1

tags = {
  Environment = "dev"
  Project     = "kafka-streaming"
  ManagedBy   = "terraform"
}
