# tablelineage
A Python package that can be used to retrieve data lineage using Databricks Data Lineage REST API

## Installation

This package is intended to be installed within Databricks as a Python library.

## Example Usage

```python
from tablelineage import ShowMeLineage

## Define the parameters
catalog_name = "<catalog_name>"
schema_name = "<schema_name>"
table_name = "<table_name>"
databricks_instance = "<databricks_instance"  # example: adb--xxxxxxxxxxx.x.azuredatabricks.net
workspace_id = "<workspace_id>"
token = "<token>"
## For notebook based jobs you can dynamically get token using the following: 
# token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

conn = ShowMeLineage(databricks_instance, workspace_id, token)
df = conn.getTableLineage(catalog_name, schema_name, table_name)
df.display()

"""
Always exclude 'NA' in the 'lineage_direction' column from the resulting dataframe,
unless you are interested in the links to the notebooks referencing specified table/view name.

"""
