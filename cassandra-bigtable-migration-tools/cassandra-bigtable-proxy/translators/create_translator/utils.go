package create_translator

import cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"

func createOptionsMap(withElement cql.IWithElementContext) map[string]string {
	results := make(map[string]string)
	// it's ok if the input is null since options are optional
	if withElement == nil || withElement.TableOptions() == nil {
		return results
	}

	for _, option := range withElement.TableOptions().AllTableOptionItem() {
		optionName := option.TableOptionName().GetText()
		optionValue := option.TableOptionValue().GetText()
		// note: this technically allows options to overwrite each other but that's probably ok
		results[optionName] = optionValue
	}
	return results
}
