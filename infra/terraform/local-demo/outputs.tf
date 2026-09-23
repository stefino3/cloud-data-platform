output "generated_file_path" {
  description = "Path of the generated file"
  value       = local_file.course_file.filename
}

output "environment" {
  description = "Current environment"
  value       = var.environment
}