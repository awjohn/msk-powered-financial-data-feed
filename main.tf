terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.2.0"
}

provider "aws" {
  region = var.aws_region
}

# MSK Cluster
module "msk_cluster" {
  source = "./modules/msk"
  
  cluster_name    = var.cluster_name
  kafka_version   = var.kafka_version
  broker_nodes    = var.broker_nodes
  instance_type   = var.instance_type
  ebs_volume_size = var.ebs_volume_size
  
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnets
  
  tags = var.tags
}

# VPC for the infrastructure
module "vpc" {
  source = "./modules/vpc"
  
  vpc_name     = var.vpc_name
  vpc_cidr     = var.vpc_cidr
  azs          = var.availability_zones
  tags         = var.tags
}

# EC2 instances for Kafka producers and consumers
module "ec2" {
  source = "./modules/ec2"
  
  vpc_id              = module.vpc.vpc_id
  subnet_ids          = module.vpc.private_subnets
  instance_type       = var.ec2_instance_type
  key_name            = var.key_name
  producer_count      = var.producer_instance_count
  consumer_count      = var.consumer_instance_count
  msk_security_group  = module.msk_cluster.security_group_id
  tags                = var.tags
}
