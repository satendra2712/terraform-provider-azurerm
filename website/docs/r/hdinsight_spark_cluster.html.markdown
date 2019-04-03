---
layout: "azurerm"
page_title: "Azure Resource Manager: azurerm_hdinsight_spark_cluster"
sidebar_current: "docs-azurerm-resource-hdinsight-spark-cluster"
description: |-
  Manages a HDInsight Spark Cluster.
---

# azurerm_hdinsight_spark_cluster

Manages a HDInsight Spark Cluster.

## Example Usage

```hcl
TODO
```

## Argument Reference

The following arguments are supported:

* `name` - (Required) Specifies the name for this HDInsight Cluster. Changing this forces a new resource to be created.

* `resource_group_name` - (Required) Specifies the name of the Resource Group in which this HDInsight Cluster should exist. Changing this forces a new resource to be created.

* `location` - (Required) Specifies the Azure Region which this HDInsight Cluster should exist. Changing this forces a new resource to be created.

* `cluster_version` - (Required) Specifies the Version of HDInsights  

* `name` - (Required) Specifies the name of the image. Changing this forces a
    new resource to be created.
* `resource_group_name` - (Required) The name of the resource group in which to create
    the image. Changing this forces a new resource to be created.
* `location` - (Required) Specified the supported Azure location where the resource exists.
    Changing this forces a new resource to be created.


## Attributes Reference

The following attributes are exported:

* `id` - The managed image ID.

## Import

Image can be imported using the `resource id`, e.g.

```shell
terraform import azurerm_image.test /subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mygroup1/providers/microsoft.compute/images/image1
```
