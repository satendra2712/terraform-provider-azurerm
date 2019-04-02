package azurerm

import (
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/services/preview/hdinsight/mgmt/2018-06-01-preview/hdinsight"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func hdinsightClusterUpdate(clusterKind string) func(d *schema.ResourceData, meta interface{}) error {
	return func(d *schema.ResourceData, meta interface{}) error {
		client := meta.(*ArmClient).hdinsightClustersClient
		ctx := meta.(*ArmClient).StopContext

		id, err := parseAzureResourceID(d.Id())
		if err != nil {
			return err
		}

		resourceGroup := id.ResourceGroup
		name := id.Path["clusters"]

		if d.HasChange("tags") {
			tags := d.Get("tags").(map[string]interface{})
			params := hdinsight.ClusterPatchParameters{
				Tags: expandTags(tags),
			}
			if _, err := client.Update(ctx, resourceGroup, name, params); err != nil {
				return fmt.Errorf("Error updating Tags for HDInsight %q Cluster %q (Resource Group %q): %+v", clusterKind, name, resourceGroup, err)
			}
		}

		if d.HasChange("roles") {
			log.Printf("[DEBUG] Resizing the HDInsight %q Cluster", clusterKind)
			rolesRaw := d.Get("roles").([]interface{})
			roles := rolesRaw[0].(map[string]interface{})
			headNodes := roles["head_node"].([]interface{})
			headNode := headNodes[0].(map[string]interface{})
			targetInstanceCount := headNode["target_instance_count"].(int)
			params := hdinsight.ClusterResizeParameters{
				TargetInstanceCount: utils.Int32(int32(targetInstanceCount)),
			}
			if _, err := client.Resize(ctx, resourceGroup, name, params); err != nil {
				return fmt.Errorf("Error resizing the HDInsight %q Cluster %q (Resource Group %q): %+v", clusterKind, name, resourceGroup, err)
			}
		}

		return nil
	}
}

func hdinsightClusterDelete(clusterKind string) func(d *schema.ResourceData, meta interface{}) error {
	return func(d *schema.ResourceData, meta interface{}) error {
		client := meta.(*ArmClient).hdinsightClustersClient
		ctx := meta.(*ArmClient).StopContext

		id, err := parseAzureResourceID(d.Id())
		if err != nil {
			return err
		}

		resourceGroup := id.ResourceGroup
		name := id.Path["clusters"]

		future, err := client.Delete(ctx, resourceGroup, name)
		if err != nil {
			return fmt.Errorf("Error deleting HDInsight %q Cluster %q (Resource Group %q): %+v", clusterKind, name, resourceGroup, err)
		}

		if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
			return fmt.Errorf("Error waiting for deletion of HDInsight %q Cluster %q (Resource Group %q): %+v", clusterKind, name, resourceGroup, err)
		}

		return nil
	}
}
