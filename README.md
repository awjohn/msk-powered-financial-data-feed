# Kafka Streaming Infrastructure

This project contains Terraform configurations to set up a complete Kafka streaming infrastructure on AWS using Amazon MSK (Managed Streaming for Kafka).

## Architecture

The infrastructure includes:
- Amazon MSK cluster with configurable broker nodes
- VPC with public and private subnets across multiple availability zones
- NAT Gateways for private subnet internet access
- EC2 instances for Kafka producers and consumers
- Security groups and IAM roles for secure access

## Prerequisites

1. AWS CLI installed and configured
2. Terraform installed (version >= 1.2.0)
3. An SSH key pair in your AWS account for EC2 instance access

## Configuration

The main configuration variables are in `terraform.tfvars`. Key settings include:

```hcl
aws_region = "us-east-1"
cluster_name = "kafka-streaming-cluster"
kafka_version = "2.8.1"
broker_nodes = 3
```

See `variables.tf` for all available configuration options.

## Deployment

1. Initialize Terraform:
```bash
terraform init
```

2. Review the planned changes:
```bash
terraform plan
```

3. Apply the configuration:
```bash
terraform key_name="your-key-pair-name" terraform apply
```

## Post-Deployment

After deployment, you can:

1. Connect to producer instances:
```bash
ssh -i your-key.pem ec2-user@<producer-ip>
./run-producer.sh
```

2. Connect to consumer instances:
```bash
ssh -i your-key.pem ec2-user@<consumer-ip>
./run-consumer.sh
```

## Module Structure

- `msk/`: Amazon MSK cluster configuration
- `vpc/`: VPC and networking components
- `ec2/`: EC2 instances for producers and consumers

## Outputs

Key infrastructure information is available through outputs:
- MSK bootstrap brokers
- Zookeeper connection string
- EC2 instance IDs and IPs
- Security group IDs

## Security

The infrastructure implements security best practices:
- Private subnets for MSK and EC2 instances
- Security groups with minimal required access
- IAM roles with least privilege
- Encryption at rest for MSK using KMS

## Clean Up

To destroy the infrastructure:
```bash
terraform destroy
```

Note: This will delete all resources created by this Terraform configuration.
