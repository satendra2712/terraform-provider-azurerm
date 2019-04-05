package azurerm

import (
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/services/preview/hdinsight/mgmt/2018-06-01-preview/hdinsight"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func hdinsightClusterUpdate(clusterKind string, readFunc func(d *schema.ResourceData, meta interface{}) error) func(d *schema.ResourceData, meta interface{}) error {
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
			headNodes := roles["worker_node"].([]interface{})
			headNode := headNodes[0].(map[string]interface{})
			targetInstanceCount := headNode["target_instance_count"].(int)
			params := hdinsight.ClusterResizeParameters{
				TargetInstanceCount: utils.Int32(int32(targetInstanceCount)),
			}
			if _, err := client.Resize(ctx, resourceGroup, name, params); err != nil {
				return fmt.Errorf("Error resizing the HDInsight %q Cluster %q (Resource Group %q): %+v", clusterKind, name, resourceGroup, err)
			}
		}

		return readFunc(d, meta)
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

func expandHDInsightRoles(input []interface{},
	headNodeDef azure.HDInsightNodeDefinition,
	workerNodeDef azure.HDInsightNodeDefinition,
	zookeeperNodeDef azure.HDInsightNodeDefinition) (*[]hdinsight.Role, error) {
	v := input[0].(map[string]interface{})

	headNodeRaw := v["head_node"].([]interface{})
	headNode, err := azure.ExpandHDInsightNodeDefinition("headnode", headNodeRaw, headNodeDef)
	if err != nil {
		return nil, fmt.Errorf("Error expanding `head_node`: %+v", err)
	}

	workerNodeRaw := v["worker_node"].([]interface{})
	workerNode, err := azure.ExpandHDInsightNodeDefinition("workernode", workerNodeRaw, workerNodeDef)
	if err != nil {
		return nil, fmt.Errorf("Error expanding `worker_node`: %+v", err)
	}

	zookeeperNodeRaw := v["zookeeper_node"].([]interface{})
	zookeeperNode, err := azure.ExpandHDInsightNodeDefinition("zookeepernode", zookeeperNodeRaw, zookeeperNodeDef)
	if err != nil {
		return nil, fmt.Errorf("Error expanding `zookeeper_node`: %+v", err)
	}

	return &[]hdinsight.Role{
		*headNode,
		*workerNode,
		*zookeeperNode,
	}, nil
}

func flattenHDInsightRoles(d *schema.ResourceData,
	input *hdinsight.ComputeProfile,
	headNodeDef azure.HDInsightNodeDefinition,
	workerNodeDef azure.HDInsightNodeDefinition,
	zookeeperNodeDef azure.HDInsightNodeDefinition) []interface{} {
	if input == nil || input.Roles == nil {
		return []interface{}{}
	}

	var existingHeadNodes, existingWorkerNodes, existingZookeeperNodes []interface{}

	existingVs := d.Get("roles").([]interface{})
	if len(existingVs) > 0 {
		existingV := existingVs[0].(map[string]interface{})

		existingHeadNodes = existingV["head_node"].([]interface{})
		existingWorkerNodes = existingV["worker_node"].([]interface{})
		existingZookeeperNodes = existingV["zookeeper_node"].([]interface{})
	}

	headNode := azure.FindHDInsightRole(input.Roles, "headnode")
	headNodes := azure.FlattenHDInsightNodeDefinition(headNode, existingHeadNodes, headNodeDef)

	workerNode := azure.FindHDInsightRole(input.Roles, "workernode")
	workerNodes := azure.FlattenHDInsightNodeDefinition(workerNode, existingWorkerNodes, workerNodeDef)

	zookeeperNode := azure.FindHDInsightRole(input.Roles, "zookeepernode")
	zookeeperNodes := azure.FlattenHDInsightNodeDefinition(zookeeperNode, existingZookeeperNodes, zookeeperNodeDef)

	return []interface{}{
		map[string]interface{}{
			"head_node":      headNodes,
			"worker_node":    workerNodes,
			"zookeeper_node": zookeeperNodes,
		},
	}
}
