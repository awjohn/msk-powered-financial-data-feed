variable "cluster_name" {
  description = "Name of the MSK cluster"
  type        = string
}

variable "kafka_version" {
  description = "Version of Kafka to deploy"
  type        = string
}

variable "broker_nodes" {
  description = "Number of broker nodes in the cluster"
  type        = number
}

variable "instance_type" {
  description = "Instance type for MSK brokers"
  type        = string
}

variable "ebs_volume_size" {
  description = "Size of EBS volume for MSK brokers (in GB)"
  type        = number
}

variable "vpc_id" {
  description = "ID of the VPC"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs"
  type        = list(string)
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
}
