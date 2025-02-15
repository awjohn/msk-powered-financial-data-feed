variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the MSK cluster"
  type        = string
  default     = "kafka-cluster"
}

variable "kafka_version" {
  description = "Version of Kafka to deploy"
  type        = string
  default     = "2.8.1"
}

variable "broker_nodes" {
  description = "Number of broker nodes in the cluster"
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "Instance type for MSK brokers"
  type        = string
  default     = "kafka.t3.small"
}

variable "ebs_volume_size" {
  description = "Size of EBS volume for MSK brokers (in GB)"
  type        = number
  default     = 100
}

variable "vpc_name" {
  description = "Name of the VPC"
  type        = string
  default     = "msk-vpc"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "ec2_instance_type" {
  description = "Instance type for EC2 instances"
  type        = string
  default     = "t3.micro"
}

variable "key_name" {
  description = "Name of the key pair for EC2 instances"
  type        = string
}

variable "producer_instance_count" {
  description = "Number of producer EC2 instances"
  type        = number
  default     = 1
}

variable "consumer_instance_count" {
  description = "Number of consumer EC2 instances"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {
    Environment = "dev"
    Project     = "kafka-streaming"
  }
}
