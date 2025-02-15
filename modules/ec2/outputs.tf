output "producer_instance_ids" {
  description = "IDs of producer EC2 instances"
  value       = aws_instance.producer[*].id
}

output "consumer_instance_ids" {
  description = "IDs of consumer EC2 instances"
  value       = aws_instance.consumer[*].id
}

output "producer_private_ips" {
  description = "Private IPs of producer EC2 instances"
  value       = aws_instance.producer[*].private_ip
}

output "consumer_private_ips" {
  description = "Private IPs of consumer EC2 instances"
  value       = aws_instance.consumer[*].private_ip
}

output "producer_security_group_id" {
  description = "ID of the producer security group"
  value       = aws_security_group.producer.id
}

output "consumer_security_group_id" {
  description = "ID of the consumer security group"
  value       = aws_security_group.consumer.id
}
