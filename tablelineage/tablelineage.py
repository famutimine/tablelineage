import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
import json
import sys


class ShowMeLineage:
    def __init__(self, databricks_instance: str, workspace_id: str):
        """
        Initialize the ShowMeLineage with Databricks instance details.

        Args:
            databricks_instance (str): The Databricks instance URL.
            workspace_id (str): The workspace ID for constructing pipeline links.
        """
        self.databricks_instance = databricks_instance
        self.workspace_id = workspace_id
        self.token = self._get_api_token()

    def _get_api_token(self) -> str:
        """Retrieve the API token securely."""
        try:
            return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except Exception as e:
            raise Exception("Failed to retrieve the API token. Ensure you have the necessary permissions.") from e

    def _parse_timestamp(self, ts_str: str) -> datetime:
        """Parse a timestamp string into a datetime object."""
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Timestamp format not recognized: {ts_str}")

    def _make_request(self, catalog_name: str, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Make a GET request to fetch table lineage data."""
        url = f"https://{self.databricks_instance}/api/2.0/lineage-tracking/table-lineage"
        payload = {
            "table_name": f"{catalog_name}.{schema_name}.{table_name}",
            "include_entity_lineage": True
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        response = requests.get(url, headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        return response.json()

    def _process_entities(self, data: Dict[str, Any], catalog_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Process the upstream and downstream data into a structured format."""
        entities = {}
        main_key = (catalog_name, schema_name, table_name, 'TABLE')
        entities[main_key] = {
            'name': table_name,
            'catalog_name': catalog_name,
            'schema_name': schema_name,
            'type': 'TABLE',
            'latest_pipeline_id': None,
            'latest_update_id': None,
            'latest_pipeline_lineage_timestamp': None,
            'lineage_direction': set(),
            'notebook_links': set()
        }

        def process_lineage(lineages: List[Dict[str, Any]], direction: str):
            for lineage in lineages:
                table_info = lineage.get('tableInfo')
                if table_info:
                    name = table_info.get('name')
                    catalog = table_info.get('catalog_name')
                    schema = table_info.get('schema_name')
                    table_type = table_info.get('table_type')
                    key = (name, catalog, schema, table_type)

                    if key not in entities:
                        entities[key] = {
                            'name': name,
                            'catalog_name': catalog,
                            'schema_name': schema,
                            'type': table_type,
                            'latest_pipeline_id': None,
                            'latest_update_id': None,
                            'latest_pipeline_lineage_timestamp': None,
                            'lineage_direction': set(),
                            'notebook_links': set()
                        }

                    entities[key]['lineage_direction'].add(direction)
                    if direction == 'upstream':
                        pipeline_infos = lineage.get('pipelineInfos', [])
                        for pipeline in pipeline_infos:
                            pipeline_id = pipeline.get('pipeline_id')
                            update_id = pipeline.get('update_id')
                            lineage_timestamp_str = pipeline.get('lineage_timestamp')
                            try:
                                lineage_timestamp = self._parse_timestamp(lineage_timestamp_str)
                            except ValueError:
                                continue

                            current_latest = entities[key]['latest_pipeline_lineage_timestamp']
                            if not current_latest or lineage_timestamp > self._parse_timestamp(current_latest):
                                entities[key]['latest_pipeline_id'] = pipeline_id
                                entities[key]['latest_update_id'] = update_id
                                entities[key]['latest_pipeline_lineage_timestamp'] = lineage_timestamp_str
                    elif direction == 'downstream':
                        notebook_infos = lineage.get('notebookInfos', [])
                        for notebook in notebook_infos:
                            notebook_id = notebook.get('notebook_id')
                            workspace_id = notebook.get('workspace_id')
                            if notebook_id and workspace_id:
                                notebook_link = f"https://{self.databricks_instance}/editor/notebooks/{notebook_id}?o={workspace_id}"
                                entities[key]['notebook_links'].add(notebook_link)
                else:
                    # Downstream notebooks not associated with specific tables
                    notebook_infos = lineage.get('notebookInfos', [])
                    for notebook in notebook_infos:
                        notebook_id = notebook.get('notebook_id')
                        workspace_id = notebook.get('workspace_id')
                        if notebook_id and workspace_id:
                            notebook_link = f"https://{self.databricks_instance}/editor/notebooks/{notebook_id}?o={workspace_id}"
                            entities[main_key]['notebook_links'].add(notebook_link)

        process_lineage(data.get('upstreams', []), 'upstream')
        process_lineage(data.get('downstreams', []), 'downstream')

        return self._convert_entities_to_list(entities, catalog_name, schema_name, table_name)

    def _convert_entities_to_list(self, entities: Dict[Any, Dict[str, Any]], catalog_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Convert the processed entities into a list of dictionaries for DataFrame creation."""
        data_list = []
        for key, entity in entities.items():
            direction = 'NA'
            if entity['lineage_direction'] == {'upstream'}:
                direction = 'upstream'
            elif entity['lineage_direction'] == {'downstream'}:
                direction = 'downstream'

            pipeline_link = None
            if entity['latest_pipeline_id'] and entity['latest_update_id']:
                pipeline_link = f"https://{self.databricks_instance}/pipelines/{entity['latest_pipeline_id']}/updates/{entity['latest_update_id']}?o={self.workspace_id}"

            notebook_links = json.dumps(sorted(entity['notebook_links'])) if entity['notebook_links'] else "null"

            data_list.append({
                'entity_catalog': catalog_name,
                'entity_schema': schema_name,
                'entity_name': table_name,
                'lineage_direction': direction,
                'catalog_name': entity['catalog_name'],
                'schema_name': entity['schema_name'],
                'table_name': entity['name'],
                'type': entity['type'],
                'latest_pipeline_id': entity['latest_pipeline_id'],
                'latest_update_id': entity['latest_update_id'],
                'latest_pipeline_lineage_timestamp': entity['latest_pipeline_lineage_timestamp'],
                'pipeline_link': pipeline_link,
                'notebook_links': notebook_links
            })
        return data_list

    def getTableLineage(self, catalog_name: str, schema_name: str, table_name: str) -> pd.DataFrame:
        """Fetch and process table lineage data into a DataFrame."""
        data = self._make_request(catalog_name, schema_name, table_name)
        data_list = self._process_entities(data, catalog_name, schema_name, table_name)
        df = pd.DataFrame(data_list)
        df['latest_pipeline_lineage_timestamp'] = pd.to_datetime(df['latest_pipeline_lineage_timestamp'], errors='coerce')
        return df
        