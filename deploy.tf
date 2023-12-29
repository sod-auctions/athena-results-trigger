terraform {
  backend "s3" {
    bucket = "sod-auctions-deployments"
    key    = "terraform/athena_results_trigger"
    region = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"
}

variable "app_name" {
  type    = string
  default = "athena_results_trigger"
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/bootstrap"
  output_path = "${path.module}/lambda_function.zip"
}

data "local_file" "lambda_zip_contents" {
  filename = data.archive_file.lambda_zip.output_path
}

data "aws_ssm_parameter" "db_connection_string" {
  name = "/db-connection-string"
}

data "aws_lambda_function" "athena_aggregation_trigger" {
  function_name = "athena_aggregation_trigger"
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.app_name}_execution_role"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Action" : "sts:AssumeRole",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Effect" : "Allow"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_exec_policy" {
  role   = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetObject",
        ],
        "Resource" : [
          "arn:aws:s3:::sod-auctions/*"
        ]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "athena_results_trigger" {
  function_name    = var.app_name
  architectures    = ["arm64"]
  memory_size      = 256
  handler          = "bootstrap"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.local_file.lambda_zip_contents.content_md5
  runtime          = "provided.al2023"
  timeout          = 60

  environment {
    variables = {
      DB_CONNECTION_STRING = data.aws_ssm_parameter.db_connection_string.value
    }
  }
}

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.athena_results_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::sod-auctions"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = "sod-auctions"

  lambda_function {
    lambda_function_arn = data.aws_lambda_function.athena_aggregation_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "data/"
    filter_suffix       = ".parquet"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.athena_results_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "results/aggregates"
    filter_suffix       = ".csv"
  }
}
