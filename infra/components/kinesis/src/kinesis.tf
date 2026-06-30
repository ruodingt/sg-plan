resource "aws_kinesis_stream" "transactions" {
  name             = "${local.prefix}-transactions"
  shard_count      = var.environment == "prod" ? 30 : 2
  retention_period = 24
  stream_mode_details { stream_mode = "PROVISIONED" }
}

resource "aws_kinesis_stream" "demographics" {
  name             = "${local.prefix}-demographics"
  shard_count      = 1
  retention_period = 24
  stream_mode_details { stream_mode = "PROVISIONED" }
}
