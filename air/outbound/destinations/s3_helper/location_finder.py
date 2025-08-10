import logging
import re
from typing import Dict, List, Optional, Tuple


class LocationFinder:
    """Helper class for discovering and managing S3 write locations in Databricks."""

    def __init__(
        self,
        log_level: str = 'INFO',
        dbutils_instance=None,
        spark_instance=None
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        if dbutils_instance is not None:
            self.dbutils = dbutils_instance
        else:
            try:
                self.dbutils = globals().get('dbutils')
                if self.dbutils is None:
                    raise NameError("dbutils not found")
            except (NameError, KeyError):
                raise RuntimeError(
                    "dbutils is not available and was not provided. Must be run in Databricks."
                )

        if spark_instance is not None:
            self.spark = spark_instance
        else:
            try:
                self.spark = globals().get('spark')
                if self.spark is None:
                    raise NameError("spark not found")
            except (NameError, KeyError):
                raise RuntimeError(
                    "spark is not available and was not provided. Must be run in Databricks."
                )

        self._workspace_id = None
        self._workspace_info = None

    @property
    def workspace_id(self) -> str:
        if self._workspace_id is None:
            try:
                self._workspace_id = (
                    self.dbutils.notebook.entry_point.getDbutils()
                    .notebook().getContext().workspaceId().get()
                )
                self.logger.info(f"Retrieved workspace ID: {self._workspace_id}")
            except Exception as e:
                raise RuntimeError(f"Failed to get workspace ID: {str(e)}")
        return self._workspace_id

    @property
    def workspace_info(self) -> Dict[str, str]:
        if self._workspace_info is None:
            self._workspace_info = self._extract_customer_and_env_from_s3()
        return self._workspace_info

    def get_customer_name(self) -> str:
        return self.workspace_info['customer_name']

    def get_environment(self) -> str:
        return self.workspace_info['environment']

    def _get_external_locations(self) -> List[Tuple[str, str]]:
        try:
            locations = self.spark.sql("SHOW EXTERNAL LOCATIONS").collect()
            return [
                (
                    str(row[0]) if len(row) > 0 else '',
                    str(row[1]) if len(row) > 1 else ''
                ) for row in locations
            ]
        except Exception as e:
            self.logger.error(f"Failed to retrieve external locations: {str(e)}")
            raise

    def _parse_s3_url(self, url: str) -> Optional[Dict[str, str]]:
        if not url.startswith("s3://"):
            return None
        match = re.match(r's3://([^/]+)/(.*)$', url)
        if match:
            return {
                'bucket': match.group(1),
                'path': match.group(2) if match.group(2) else ''
            }
        return None

    def _extract_customer_and_env_from_s3(self) -> Dict[str, str]:
        self.logger.info(
            f"Searching for S3 external locations with Workspace ID: {self.workspace_id}"
        )
        try:
            locations = self._get_external_locations()
            for name, url in locations:
                if f"workspaceId={self.workspace_id}" in url and url.startswith("s3://"):
                    parsed = self._parse_s3_url(url)
                    if parsed:
                        bucket_name = parsed['bucket']
                        self.logger.info(
                            f"Found matching S3 location - URL: {url}, Bucket: {bucket_name}"
                        )
                        bucket_parts = bucket_name.split('-')
                        if 'databricks' in bucket_parts and 'root' in bucket_parts:
                            databricks_idx = bucket_parts.index('databricks')
                            root_idx = bucket_parts.index('root')
                            customer_name = '-'.join(bucket_parts[:databricks_idx])
                            env_name = '-'.join(bucket_parts[root_idx+1:])
                            result = {
                                'workspace_id': self.workspace_id,
                                'url': url,
                                'bucket': bucket_name,
                                'customer_name': customer_name,
                                'environment': env_name,
                                'location_name': name,
                            }
                            self.logger.info(
                                f"Extracted - Customer: {customer_name}, Environment: {env_name}"
                            )
                            return result
            raise ValueError(
                f"No S3 external location found for workspace ID: {self.workspace_id}"
            )
        except Exception as e:
            self.logger.error(f"Error extracting customer and environment: {str(e)}")
            raise

    def find_target_s3_location(self, customer_name: str, environment: str, keyword: str) -> Dict[str, str]:
        self.logger.info(
            f"Searching for S3 location - Customer: {customer_name}, "
            f"Environment: {environment}, Keyword: {keyword}"
        )
        try:
            locations = self._get_external_locations()
            for name, url in locations:
                parsed = self._parse_s3_url(url)
                if not parsed:
                    continue
                bucket_name = parsed['bucket']
                path = parsed['path']
                if customer_name.lower() not in bucket_name.lower():
                    continue
                if keyword.lower() not in bucket_name.lower():
                    continue
                bucket_lower = bucket_name.lower()
                env_pattern = f'-{environment.lower()}-'
                if env_pattern in bucket_lower:
                    return {
                        'name': name,
                        'url': url,
                        'bucket': bucket_name,
                        'path': path
                    }
            raise ValueError(
                "No S3 location found matching - "
                f"Customer: {customer_name}, Environment: {environment}, Keyword: {keyword}"
            )
        except Exception as e:
            self.logger.error(f"Error finding target S3 location: {str(e)}")
            raise

    def get_write_location_for_keyword(self, keyword: str, custom_workspace_id: Optional[str] = None) -> Dict[str, str]:
        if custom_workspace_id:
            self._workspace_id = custom_workspace_id
            self._workspace_info = None
        customer_name = self.get_customer_name()
        environment = self.get_environment()
        target_location = self.find_target_s3_location(customer_name, environment, keyword)
        return {
            'customer': customer_name,
            'environment': environment,
            'keyword': keyword,
            'write_url': target_location['url'],
            'bucket': target_location['bucket'],
            'path': target_location['path'],
            'location_name': target_location['name'],
        }

    def get_all_s3_locations(
        self,
        filter_customer: bool = True,
        filter_environment: bool = True
    ) -> List[Dict[str, str]]:
        locations = self._get_external_locations()
        customer_name = self.get_customer_name() if filter_customer else None
        environment = self.get_environment() if filter_environment else None
        result = []
        for name, url in locations:
            parsed = self._parse_s3_url(url)
            if not parsed:
                continue
            bucket_name = parsed['bucket']
            if filter_customer and customer_name.lower() not in bucket_name.lower():
                continue
            if filter_environment:
                env_pattern = f'-{environment.lower()}-'
                if env_pattern not in bucket_name.lower():
                    continue
            result.append({
                'name': name,
                'url': url,
                'bucket': bucket_name,
                'path': parsed['path']
            })
        return result

    def reset_cache(self):
        self._workspace_id = None
        self._workspace_info = None
        self.logger.info("Cache reset completed")
