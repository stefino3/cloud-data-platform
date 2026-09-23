terraform {
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

resource "local_file" "course_file" {
  filename = "${path.module}/generated/terraform.txt"

  content = <<EOT
This file was created by Terraform.

Project: ${var.project_name}
Environment: ${var.environment}
EOT
}