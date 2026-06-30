resource "aws_sns_topic" "ml_alerts" {
  name = "fraud-ml-alerts-${var.environment}"
}

resource "aws_sns_topic" "tech_alerts" {
  name = "fraud-tech-alerts-${var.environment}"
}
