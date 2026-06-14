# AMI Ubuntu 22.04 LTS (x86_64) la plus recente, publiee par Canonical.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Security group : SSH entrant restreint a ton IP, tout sortant autorise
# (Binance WebSocket + Snowpipe Streaming sortent en 443).
# Le port 8000 (/healthz) reste INTERNE -> volontairement aucune regle entrante.
resource "aws_security_group" "consumer" {
  name        = "crypto-consumer-sg"
  description = "SSH in (restricted) + all egress for crypto consumer"

  ingress {
    description = "SSH (restreint a ton IP)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  egress {
    description = "Tout sortant (Binance WS + Snowflake 443)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Project = "crypto-realtime" }
}

# La VM consumer. user_data bootstrappe TOUT sauf les secrets (cf. user_data.sh).
resource "aws_instance" "consumer" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.consumer.id]

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    repo_url = var.repo_url
  })

  tags = {
    Name    = "CryptoServer"
    Project = "crypto-realtime"
  }
}
