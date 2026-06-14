variable "region" {
  description = "Region AWS. DOIT etre NON-US : Binance bloque les IP US (HTTP 451)."
  type        = string
  default     = "eu-west-3" # Paris
}

variable "instance_type" {
  description = "Type d'instance EC2. t2.micro = 1 vCPU / 1 Go, free tier eligible."
  type        = string
  default     = "t2.micro"
}

variable "key_name" {
  description = "Nom de la key pair EC2 EXISTANTE (creee a la console) utilisee pour le SSH."
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "CIDR autorise en SSH entrant. Mets TON IP en /32 (https://ifconfig.me)."
  type        = string
}

variable "repo_url" {
  description = "URL du repo git clone par le bootstrap (user_data)."
  type        = string
  default     = "https://github.com/Mo-oM-1/crypto-realtime-snowflake.git"
}

variable "root_volume_gb" {
  description = "Taille du disque racine (Go). <= 30 pour rester dans le free tier."
  type        = number
  default     = 8
}
