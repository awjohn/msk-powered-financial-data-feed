resource "aws_security_group" "msk" {
  name_prefix = "msk-sg-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.selected.cidr_block]
  }

  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.selected.cidr_block]
  }

  tags = var.tags
}

resource "aws_msk_cluster" "kafka" {
  cluster_name           = var.cluster_name
  kafka_version         = var.kafka_version
  number_of_broker_nodes = var.broker_nodes

  broker_node_group_info {
    instance_type   = var.instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]
    
    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
  }

  tags = var.tags
}

resource "aws_kms_key" "msk" {
  description = "KMS key for MSK cluster encryption"
  tags        = var.tags
}

data "aws_vpc" "selected" {
  id = var.vpc_id
}
