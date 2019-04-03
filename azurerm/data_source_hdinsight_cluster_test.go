package azurerm

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform/helper/acctest"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
)

func TestAccDataSourceAzureRMHDInsightCluster_spark(t *testing.T) {
	dataSourceName := "data.azurerm_hdinsight_cluster.test"
	rInt := tf.AccRandTimeInt()
	rString := strings.ToLower(acctest.RandString(11))
	location := testLocation()

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccDataSourceHDInsightCluster_spark(rInt, rString, location),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(dataSourceName, "kind", "Spark"),
					resource.TestCheckResourceAttr(dataSourceName, "tier", "Standard"),
					resource.TestCheckResourceAttrSet(dataSourceName, "https_endpoint"),
					resource.TestCheckResourceAttrSet(dataSourceName, "ssh_endpoint"),
				),
			},
		},
	})
}

func testAccDataSourceHDInsightCluster_spark(rInt int, rString, location string) string {
	template := testAccAzureRMHDInsightSparkCluster_basic(rInt, rString, location)
	return fmt.Sprintf(`
%s

data "azurerm_hdinsight_cluster" "test" {
  name                = "${azurerm_hdinsight_spark_cluster.test.name}"
  resource_group_name = "${azurerm_hdinsight_spark_cluster.test.resource_group_name}"
}
`, template)
}
