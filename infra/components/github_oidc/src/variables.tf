variable "aws_region"        { type = string; default = "ap-southeast-2" }
variable "github_org"        { type = string }
variable "github_repo"       { type = string }
variable "restrict_to_main"  {
  description = "true for sit/prod — only main branch can assume the deploy role"
  type        = bool
  default     = false
}
