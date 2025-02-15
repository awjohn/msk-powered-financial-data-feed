output "bootstrap_brokers" {
  description = "MSK cluster bootstrap brokers"
  value       = aws_msk_cluster.kafka.bootstrap_brokers
}

output "zookeeper_connect_string" {
  description = "MSK cluster zookeeper connection string"
  value       = aws_msk_cluster.kafka.zookeeper_connect_string
}

output "security_group_id" {
  description = "ID of the MSK security group"
  value       = aws_security_group.msk.id
}
