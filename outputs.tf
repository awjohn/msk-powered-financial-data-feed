output "msk_bootstrap_brokers" {
  description = "MSK cluster bootstrap brokers"
  value       = module.msk_cluster.bootstrap_brokers
}

output "msk_zookeeper_connect_string" {
  description = "MSK cluster zookeeper connection string"
  value       = module.msk_cluster.zookeeper_connect_string
}

output "vpc_id" {
  description = "ID of the VPC"
  value       = module.vpc.vpc_id
}

output "producer_instance_ids" {
  description = "IDs of producer EC2 instances"
  value       = module.ec2.producer_instance_ids
}

output "consumer_instance_ids" {
  description = "IDs of consumer EC2 instances"
  value       = module.ec2.consumer_instance_ids
}
