variable "name_prefix" { type = string }
variable "cidr_block" { type = string }
variable "availability_zones" { type = list(string) }

locals {
  public_cidrs       = [for i, _ in var.availability_zones : cidrsubnet(var.cidr_block, 8, i + 1)]
  private_app_cidrs  = [for i, _ in var.availability_zones : cidrsubnet(var.cidr_block, 8, i + 11)]
  private_data_cidrs = [for i, _ in var.availability_zones : cidrsubnet(var.cidr_block, 8, i + 21)]
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr_block
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

resource "aws_subnet" "public" {
  count                   = length(var.availability_zones)
  vpc_id                  = aws_vpc.this.id
  cidr_block              = local.public_cidrs[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true
  tags = {
    Name = "${var.name_prefix}-public-${count.index + 1}"
    Tier = "public"
  }
}

resource "aws_subnet" "private_app" {
  count             = length(var.availability_zones)
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.private_app_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]
  tags = {
    Name = "${var.name_prefix}-private-app-${count.index + 1}"
    Tier = "private-app"
  }
}

resource "aws_subnet" "private_data" {
  count             = length(var.availability_zones)
  vpc_id            = aws_vpc.this.id
  cidr_block        = local.private_data_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]
  tags = {
    Name = "${var.name_prefix}-private-data-${count.index + 1}"
    Tier = "private-data"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

resource "aws_eip" "nat" {
  domain = "vpc"
  tags = {
    Name = "${var.name_prefix}-nat-eip"
  }
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags = {
    Name = "${var.name_prefix}-nat"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = { Name = "${var.name_prefix}-rt-public" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }
  tags = { Name = "${var.name_prefix}-rt-private" }
}

resource "aws_route_table_association" "public" {
  count          = length(var.availability_zones)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private_app" {
  count          = length(var.availability_zones)
  subnet_id      = aws_subnet.private_app[count.index].id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_data" {
  count          = length(var.availability_zones)
  subnet_id      = aws_subnet.private_data[count.index].id
  route_table_id = aws_route_table.private.id
}

# S3 Gateway VPC endpoint (skip NAT for S3 traffic — huge cost saver)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  route_table_ids   = [aws_route_table.private.id]
  vpc_endpoint_type = "Gateway"
  tags              = { Name = "${var.name_prefix}-s3-endpoint" }
}

data "aws_region" "current" {}

output "vpc_id" { value = aws_vpc.this.id }
output "public_subnet_ids" { value = aws_subnet.public[*].id }
output "private_app_subnet_ids" { value = aws_subnet.private_app[*].id }
output "private_data_subnet_ids" { value = aws_subnet.private_data[*].id }
