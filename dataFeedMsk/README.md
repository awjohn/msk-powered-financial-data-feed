# MSK Infrastructure Stack

This CDK stack creates the infrastructure for an MSK (Managed Streaming for Kafka) cluster with associated resources.

## Key Components

### VPC Configuration
- Public and private subnets across 2 AZs
- NAT gateways for private subnet connectivity
- Configurable CIDR ranges and subnet masks

### Security Groups
- MSK Cluster security group
- EC2 instances security group
- Apache Flink security group
- Consolidated security rules for better management

### Storage
- S3 bucket for artifacts with versioning
- Automated cleanup on stack deletion
- Server-side encryption enabled

### Security
- KMS key for encryption
- Secrets Manager for credentials
- SASL/SCRAM authentication support
- No hardcoded credentials

## Usage

1. Configure parameters in `parameters.py`:
```python
project = "your-project"
env = "dev"
app = "your-app"
```

2. Deploy the stack:
```bash
cdk deploy
```

3. Access outputs:
- VPC ID
- Security Group IDs
- S3 Bucket name
- KMS Key ARN

## Best Practices Implemented

1. Security:
- All sensitive data in Secrets Manager
- KMS encryption for secrets
- Proper security group rules
- No public access to S3 buckets

2. Networking:
- Private subnets for sensitive resources
- Public subnets only where needed
- Controlled ingress/egress rules

3. Resource Management:
- Proper cleanup policies
- Resource versioning
- Automated artifact deployment

4. Monitoring & Operations:
- Resource tagging for cost allocation
- Exported outputs for cross-stack references
- Clear resource naming convention

## Configuration

Key parameters can be modified in `parameters.py`:
- Network configuration (CIDR ranges, AZs)
- Instance types and counts
- Security group ports
- MSK configuration
- Apache Flink settings

## Architecture

```
                                    ┌─────────────┐
                                    │             │
                                    │    KMS      │
                                    │             │
                                    └─────────────┘
                                          │
                                          ▼
┌─────────────┐               ┌─────────────────┐
│             │               │                 │
│    MSK      │◄─────────────►│  Secrets       │
│   Cluster   │               │  Manager        │
│             │               │                 │
└─────────────┘               └─────────────────┘
       ▲
       │
       ▼
┌─────────────┐               ┌─────────────────┐
│             │               │                 │
│    EC2      │               │     S3          │
│  Instances  │               │   Artifacts     │
│             │               │                 │
└─────────────┘               └─────────────────┘