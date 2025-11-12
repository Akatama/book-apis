variable "ResourceBaseName" {
  type        = string
  description = "The base name of the resource"
}

variable "ResourceGroupName" {
  type        = string
  description = "Name of the resource group for the Database and the Blob Storage"
  default     = "blogifier"
}

variable "Location" {
  type    = string
  default = "Central US"
}

variable "VirtualNetworkAddressSpace" {
  type        = string
  default     = "10.2.0.0/16"
  description = "The Address space of the virtual network"
}
variable "AppSubnetName" {
  type        = string
  default     = "app"
  description = "The name of the Subnet where we will attach our app service"
}

variable "AppSubnetPrefix" {
  type        = string
  default     = "10.2.0.0/24"
  description = "The Address Prefix for the app service subnet"
}

variable "DBSubnetName" {
  type        = string
  default     = "database"
  description = "The name of the Subnet where we will attach our database"
}

variable "DBSubnetPrefix" {
  type        = string
  default     = "10.2.1.0/24"
  description = "The Address Prefix for the DB subneet"
}
variable "KeyVaultName" {
  type    = string
  default = "jimmykeys"
}

variable "KeyVaultResourceGroup" {
  type    = string
  default = "key-vault"
}

variable "AdminPasswordSecretName" {
  type    = string
  default = "mySqlAdminPassword"
}
