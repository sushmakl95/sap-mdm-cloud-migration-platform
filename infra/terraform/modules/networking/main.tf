variable "name_prefix" { type = string }
variable "vpc_id" { type = string }
variable "public_subnet_ids" { type = list(string) }
variable "private_app_subnet_ids" { type = list(string) }
variable "private_data_subnet_ids" { type = list(string) }

resource "aws_security_group" "app" {
  name        = "${var.name_prefix}-app-sg"
  description = "App tier (Glue/Lambda/DMS workers) egress + intra-tier"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All egress for SAP + AWS API calls"
  }

  tags = { Name = "${var.name_prefix}-app-sg" }
}

resource "aws_security_group_rule" "app_self_ingress" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.app.id
  source_security_group_id = aws_security_group.app.id
  description              = "Intra-app-tier communication"
}

resource "aws_security_group" "data" {
  name        = "${var.name_prefix}-data-sg"
  description = "Data tier (RDS + Redshift) — only accepts from app tier"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
    description     = "Postgres from app tier"
  }

  ingress {
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
    description     = "Redshift from app tier"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Egress for CloudWatch metrics + KMS"
  }

  tags = { Name = "${var.name_prefix}-data-sg" }
}

output "app_sg_id" { value = aws_security_group.app.id }
output "data_sg_id" { value = aws_security_group.data.id }
