variable "ResourceBaseName" {
  type        = string
  description = "The base name of the resource"
}

variable "ResourceGroupName" {
  type        = string
  description = "Name of the resource group for the Database and the Blob Storage"
  default     = "booksapi"
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

variable "AdminUserName" {
  type        = string
  description = "Administrator username for the DB"
}

variable "ServerEdition" {
  type        = string
  description = "The tier of the particular SKU. Valid values are B for Burstable, GP for GeneralPurpose and MO for Memory Optimized. High Availability is available for GeneralPurpose and MemoryOptimized sku."
  default     = "B"
}

variable "DBSkuName" {
  type        = string
  default     = "Standard_B1ms"
  description = "The name of the sku, e.g. Standard_D32ds_v4."
}

variable "IPAddress" {
  type        = string
  description = "IP Address for Firewall access"
}
