output "transactions_stream_name" { value = aws_kinesis_stream.transactions.name }
output "transactions_stream_arn"  { value = aws_kinesis_stream.transactions.arn }
output "demographics_stream_name" { value = aws_kinesis_stream.demographics.name }
output "demographics_stream_arn"  { value = aws_kinesis_stream.demographics.arn }
