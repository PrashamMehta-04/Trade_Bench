terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
  }
}

provider "kubernetes" {
  config_path = "~/.kube/config"
}

provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}

resource "kubernetes_namespace" "trade_bench" {
  metadata {
    name = "trade-bench"
  }
}

resource "helm_release" "trade_bench" {
  name       = "trade-bench"
  namespace  = kubernetes_namespace.trade_bench.metadata[0].name
  chart      = "../helm/trade-bench"
  
  # Ensure NATS is available (could be deployed via another helm chart)
  set {
    name  = "nats.url"
    value = "nats://nats.default.svc.cluster.local:4222"
  }
}
