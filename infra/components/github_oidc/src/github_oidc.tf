# ── GitHub Actions OIDC provider ──────────────────────────────────────────────
# Applied once per account. Controls which GitHub refs can assume the deploy
# role — dev allows all branches (PRs need lint/test), sit/prod restrict to main.

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  # AWS has pinned GitHub's CA internally since 2023 — this value is no longer
  # validated, but Terraform requires a non-empty list.
  thumbprint_list = ["ffffffffffffffffffffffffffffffffffffffff"]
}

locals {
  oidc_arn    = aws_iam_openid_connect_provider.github.arn
  repo_prefix = "repo:${var.github_org}/${var.github_repo}"

  # dev trusts all branches; sit/prod restrict to main
  trusted_refs = var.restrict_to_main ? ["${local.repo_prefix}:ref:refs/heads/main"] \
                                      : ["${local.repo_prefix}:*"]
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals { type = "Federated"; identifiers = [local.oidc_arn] }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = local.trusted_refs
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "github-actions-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_trust.json
}

resource "aws_iam_role_policy_attachment" "github_actions_admin" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

output "deploy_role_arn" {
  value = aws_iam_role.github_actions.arn
}
