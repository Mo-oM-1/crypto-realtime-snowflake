output "public_ip" {
  description = "IP publique de la VM consumer."
  value       = aws_instance.consumer.public_ip
}

output "ssh_command" {
  description = "Commande SSH (adapte le chemin de ta cle .pem)."
  value       = "ssh -i ~/Downloads/${var.key_name}.pem ubuntu@${aws_instance.consumer.public_ip}"
}

output "scp_secrets_command" {
  description = "Copie des secrets vers la VM apres l'apply (a lancer depuis infra/)."
  value       = "scp -i ~/Downloads/${var.key_name}.pem ../ingestion/profile.json ../ingestion/rsa_key.p8 ubuntu@${aws_instance.consumer.public_ip}:~/crypto-realtime-snowflake/ingestion/"
}
