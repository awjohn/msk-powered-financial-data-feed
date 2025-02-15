# Producer EC2 instances
resource "aws_instance" "producer" {
  count         = var.producer_count
  ami           = data.aws_ami.amazon_linux_2.id
  instance_type = var.instance_type
  subnet_id     = var.subnet_ids[count.index % length(var.subnet_ids)]
  key_name      = var.key_name

  vpc_security_group_ids = [aws_security_group.producer.id]

  user_data = templatefile("${path.module}/templates/producer_userdata.sh", {
    msk_security_group = var.msk_security_group
  })

  tags = merge(var.tags, {
    Name = "kafka-producer-${count.index + 1}"
    Role = "producer"
  })
}

# Consumer EC2 instances
resource "aws_instance" "consumer" {
  count         = var.consumer_count
  ami           = data.aws_ami.amazon_linux_2.id
  instance_type = var.instance_type
  subnet_id     = var.subnet_ids[count.index % length(var.subnet_ids)]
  key_name      = var.key_name

  vpc_security_group_ids = [aws_security_group.consumer.id]

  user_data = templatefile("${path.module}/templates/consumer_userdata.sh", {
    msk_security_group = var.msk_security_group
  })

  tags = merge(var.tags, {
    Name = "kafka-consumer-${count.index + 1}"
    Role = "consumer"
  })
}

# Security group for producer instances
resource "aws_security_group" "producer" {
  name_prefix = "kafka-producer-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "kafka-producer-sg"
  })
}

# Security group for consumer instances
resource "aws_security_group" "consumer" {
  name_prefix = "kafka-consumer-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "kafka-consumer-sg"
  })
}

# Latest Amazon Linux 2 AMI
data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# IAM role for EC2 instances
resource "aws_iam_role" "ec2_role" {
  name = "kafka-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# IAM instance profile
resource "aws_iam_instance_profile" "ec2_profile" {
  name = "kafka-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# Allow EC2 instances to access MSK
resource "aws_security_group_rule" "msk_access" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9092
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.producer.id
  security_group_id        = var.msk_security_group
}
