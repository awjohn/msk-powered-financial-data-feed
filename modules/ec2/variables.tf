variable "vpc_id" {
  description = "ID of the VPC"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs"
  type        = list(string)
}

variable "instance_type" {
  description = "Instance type for EC2 instances"
  type        = string
}

variable "key_name" {
  description = "Name of the key pair for EC2 instances"
  type        = string
}

variable "producer_count" {
  description = "Number of producer EC2 instances"
  type        = number
}

variable "consumer_count" {
  description = "Number of consumer EC2 instances"
  type        = number
}

variable "msk_security_group" {
  description = "ID of the MSK security group"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
}
