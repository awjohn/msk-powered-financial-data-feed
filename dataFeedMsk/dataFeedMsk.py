from constructs import Construct
from aws_cdk import (
    Stack, CfnOutput, RemovalPolicy,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_msk as msk,
    aws_secretsmanager as secretsmanager,
    aws_kms as kms,
    aws_s3_deployment as s3deployment,
    Aws as AWS
)
from . import parameters

class dataFeedMsk(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Resource naming helper
        def name_with_prefix(resource_type: str) -> str:
            return f"{parameters.project}-{parameters.env}-{parameters.app}-{resource_type}"

        # Standard tags for all resources
        self.tags = {
            "project": parameters.project,
            "env": parameters.env,
            "app": parameters.app
        }

        # VPC with simplified configuration
        vpc = ec2.Vpc(self, "vpc",
            vpc_name=name_with_prefix("vpc"),
            max_azs=2,
            ip_addresses=ec2.IpAddresses.cidr(parameters.cidrRange),
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name=name_with_prefix("public"),
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=parameters.cidrMaskForSubnets
                ),
                ec2.SubnetConfiguration(
                    name=name_with_prefix("private"),
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=parameters.cidrMaskForSubnets
                )
            ],
            nat_gateways=parameters.numberOfNatGateways
        )

        # Security Groups with consolidated rules
        sg_msk = ec2.SecurityGroup(self, "sgMskCluster",
            vpc=vpc,
            security_group_name=name_with_prefix("msk-cluster"),
            description="Security group for MSK Cluster"
        )

        sg_ec2 = ec2.SecurityGroup(self, "sgEc2MskCluster",
            vpc=vpc,
            security_group_name=name_with_prefix("ec2-msk"),
            description="Security group for EC2 MSK Cluster"
        )

        sg_flink = ec2.SecurityGroup(self, "sgApacheFlink",
            vpc=vpc,
            security_group_name=name_with_prefix("apache-flink"),
            description="Security group for Apache Flink"
        )

        # Allow SSH access to EC2 instances
        sg_ec2.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(22),
            description="SSH access"
        )

        # MSK communication rules
        for port in range(parameters.sgKafkaInboundPort, parameters.sgKafkaOutboundPort + 1):
            sg_msk.add_ingress_rule(
                peer=ec2.Peer.security_group_id(sg_ec2.security_group_id),
                connection=ec2.Port.tcp(port),
                description=f"Kafka port {port}"
            )
            sg_msk.add_ingress_rule(
                peer=ec2.Peer.security_group_id(sg_flink.security_group_id),
                connection=ec2.Port.tcp(port),
                description=f"Flink to MSK port {port}"
            )

        # S3 Buckets
        artifacts_bucket = s3.Bucket(self, "artifactsBucket",
            bucket_name=f"{parameters.project}-{parameters.env}-artifacts-{AWS.REGION}-{AWS.ACCOUNT_ID}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Deploy artifacts
        source_bucket = s3.Bucket.from_bucket_name(
            self, "sourceBucket",
            parameters.s3BucketName
        )

        s3deployment.BucketDeployment(self, "artifactsDeployment",
            sources=[s3deployment.Source.bucket(source_bucket, 'BDB-3696/dataFeedMskArtifacts.zip')],
            destination_bucket=artifacts_bucket
        )

        # KMS Key for encryption
        kms_key = kms.Key(self, "kmsKey",
            alias=name_with_prefix("sasl-scram"),
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Secrets for MSK and OpenSearch
        secrets = {
            "producer": parameters.mskProducerUsername,
            "consumer": parameters.mskConsumerUsername,
            "opensearch": None
        }

        for secret_id, username in secrets.items():
            template = f'{{"username": "{username}"}}' if username else None
            secret = secretsmanager.Secret(self, f"{secret_id}Secret",
                secret_name=name_with_prefix(f"{secret_id}-secret"),
                generate_secret_string=secretsmanager.SecretStringGenerator(
                    generate_string_key="password",
                    secret_string_template=template,
                    exclude_punctuation=True
                ),
                encryption_key=kms_key
            )
            setattr(self, f"{secret_id}Secret", secret)

        # Outputs
        outputs = {
            "vpcId": vpc.vpc_id,
            "sgEc2Id": sg_ec2.security_group_id,
            "sgMskId": sg_msk.security_group_id,
            "sgFlinkId": sg_flink.security_group_id,
            "artifactsBucketName": artifacts_bucket.bucket_name,
            "kmsKeyArn": kms_key.key_arn
        }

        for key, value in outputs.items():
            CfnOutput(self, key,
                value=value,
                description=f"{key} for {parameters.app}",
                export_name=name_with_prefix(key)
            )
