terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

variable "localstack_endpoint" {
  description = "LocalStack edge endpoint used for every AWS API call."
  type        = string
  default     = "http://eval-infra-localstack:4566"
}

variable "aws_region" {
  description = "Region the ShopFlow fulfilment resources live in."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment tag."
  type        = string
  default     = "eval"
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    sns = var.localstack_endpoint
    sqs = var.localstack_endpoint
    sts = var.localstack_endpoint
  }

  default_tags {
    tags = {
      Project     = "shopflow"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

resource "aws_sns_topic" "shipment_events" {
  name         = "shopflow-shipment-events"
  display_name = "ShopFlow shipment events"

  tags = {
    Domain = "fulfilment"
  }
}

resource "aws_sqs_queue" "tracking_queue" {
  name                       = "tracking-service-queue"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 10
  max_message_size           = 262144

  tags = {
    Domain = "fulfilment"
    Owner  = "tracking-service"
  }
}

resource "aws_sqs_queue_policy" "tracking_queue" {
  queue_url = aws_sqs_queue.tracking_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowShipmentEventsTopicToSendMessages"
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.tracking_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.shipment_events.arn
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "tracking_sub" {
  topic_arn            = aws_sns_topic.shipment_events.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.tracking_queue.arn
  raw_message_delivery = false

  depends_on = [aws_sqs_queue_policy.tracking_queue]
}

output "shipment_sns_arn" {
  description = "ARN consumed by shipping-service as SHIPMENT_SNS_ARN."
  value       = aws_sns_topic.shipment_events.arn
}

output "tracking_sqs_url" {
  description = "Queue URL consumed by tracking-service as TRACKING_SQS_URL."
  value       = aws_sqs_queue.tracking_queue.id
}

output "tracking_sqs_arn" {
  description = "Queue ARN subscribed to the shipment events topic."
  value       = aws_sqs_queue.tracking_queue.arn
}

output "tracking_subscription_arn" {
  description = "ARN of the SNS to SQS subscription."
  value       = aws_sns_topic_subscription.tracking_sub.arn
}
