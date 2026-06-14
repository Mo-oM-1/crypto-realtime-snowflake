# Infrastructure (Terraform)

Provisionne le consumer temps reel en **Infrastructure-as-Code** : VM EC2 (free tier),
security group (SSH restreint), et bootstrap complet via `user_data` (swap, Python,
clone, venv, service systemd). Reproductible d'un `terraform apply`.

> ⚠️ **Region NON-US obligatoire** : Binance bloque les IP US (`HTTP 451`). Garde une region UE.

## Prerequis

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- AWS CLI configure avec une cle d'acces IAM ayant les droits EC2 (`aws configure`)
- Une **key pair EC2** deja creee dans la region cible (pour le SSH)

## Usage

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # puis edite : key_name + ton IP/32
terraform init
terraform plan      # verifie ce qui va etre cree
terraform apply     # cree SG + VM (bootstrap auto via user_data)

# Copie les secrets (NON versionnes) sur la VM, puis le service demarre :
terraform output -raw scp_secrets_command | bash
# (ou copie/colle la commande affichee par `terraform output scp_secrets_command`)
```

Verifier :

```bash
eval "$(terraform output -raw ssh_command)"
# sur la VM :
systemctl status crypto-ingest     # active (running)
curl localhost:8000/healthz        # {"status":"ok",...}
```

Detruire :

```bash
terraform destroy
```

## Notes

- **Secrets** (`profile.json`, `rsa_key.p8`) : jamais dans l'IaC ni au repo. Copies par
  `scp` apres l'apply (cf. ci-dessus). En vraie prod -> AWS Secrets Manager / SSM, recuperes
  par le `user_data` au boot. C'est une limite assumee (cf. section *Limites connues* du README racine).
- **State** : local (`terraform.tfstate`, gitignore). En equipe -> backend S3 + lock DynamoDB
  (voir le bloc commente dans `versions.tf`).
- **Free tier** : `t2.micro` + disque `<= 30 Go`. Garde un budget d'alerte AWS active.
