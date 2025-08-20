import logging
import re
from pyspark.sql import SparkSession


class S3AccessChecker:
    """S3 Access Checker for Unity Catalog External Locations"""

    def __init__(self, spark=None):
        """
        Initialize S3 Access Checker

        Parameters:
        - spark: SparkSession instance
        """
        self.spark = spark or SparkSession.builder.getOrCreate()

    def list_external_locations(self):
        """
        List all Unity Catalog external locations

        Returns:
        - list: List of external location information
        """
        try:
            locations = []

            # Query external locations using SHOW EXTERNAL LOCATIONS
            result = self.spark.sql("SHOW EXTERNAL LOCATIONS").collect()
            for row in result:
                location_info = {
                    'name': row[0] if len(row) > 0 else 'N/A',
                    'url': row[1] if len(row) > 1 else 'N/A',
                    'credential_name': row[2] if len(row) > 2 else 'N/A'
                }
                locations.append(location_info)
            return locations

        except Exception as e:
            logging.warning(f"Unable to retrieve external locations: {str(e)}")
            logging.warning("This might be due to:")
            logging.warning("          - Insufficient permissions to view Unity Catalog metadata")
            logging.warning("          - Unity Catalog not being properly configured")
            logging.warning("          - Running in an environment without Unity Catalog support")
            return []

    def normalize_s3_path(self, path):
        """
        Normalize S3 path to standard format

        Parameters:
        - path: S3 path to normalize

        Returns:
        - str: Normalized S3 path
        """
        # Remove trailing slashes
        path = path.rstrip('/')

        # Ensure s3:// or s3a:// prefix (case insensitive)
        path_lower = path.lower()
        if not path_lower.startswith(('s3://', 's3a://')):
            path = f"s3://{path}"

        # Convert s3a:// to s3:// for consistency (case insensitive)
        path = re.sub(r's3a://', 's3://', path, flags=re.IGNORECASE)

        return path

    def check_path_access(self, s3_path):
        """
        Check if S3 path is accessible through Unity Catalog external locations

        Parameters:
        - s3_path: S3 path to check

        Returns:
        - dict: Access check result with status and recommendations
        """
        result = {
            'accessible': False,
            'matching_location': None,
            'available_locations': [],
            'recommendations': []
        }

        try:
            # Normalize the input path
            normalized_path = self.normalize_s3_path(s3_path)

            # Get all external locations
            external_locations = self.list_external_locations()

            # Store all available locations
            result['available_locations'] = external_locations

            # If we couldn't retrieve locations, provide different recommendations
            if not external_locations:
                result['recommendations'].extend([
                    "[WARNING] Unable to verify S3 access through Unity Catalog external locations.",
                    "[INFO] Recommendations:",
                    "       1. Ensure you have proper Unity Catalog permissions",
                    "       2. Verify that external locations are configured for your workspace",
                    "       3. Contact your administrator to confirm S3 access permissions",
                    f"       4. Target S3 path: {s3_path}",
                    "\n[NOTE] The system will attempt to write to the specified location.",
                    "       If the write fails, it's likely due to missing permissions."
                ])

                # In this case, we'll return accessible as "unknown" rather than false
                result['accessible'] = None  # Unknown state
                return result

            # Check if path matches any external location
            for location in external_locations:
                location_url = self.normalize_s3_path(location['url'])

                # Check if the input path starts with the external location URL
                if normalized_path.startswith(location_url):
                    result['accessible'] = True
                    result['matching_location'] = location
                    break

            # Generate recommendations for known locations
            if result['accessible']:
                result['recommendations'].extend([
                    f"[SUCCESS] The S3 path '{s3_path}' is accessible through Unity Catalog.",
                    f"[INFO] Matching external location: {result['matching_location']['name']}"
                ])
            else:
                recommendations_list = [
                    f"[WARNING] The S3 path '{s3_path}' is not found in Unity Catalog external locations.",
                    "[INFO] Available external locations:"
                ]
                for loc in external_locations:
                    recommendations_list.append(f"       - {loc['name']}: {loc['url']}")
                recommendations_list.extend([
                    "\n[INFO] Recommendations:",
                    "       1. Use one of the available external locations listed above",
                    "       2. Contact your administrator to create a new external location for this path"
                ])
                result['recommendations'].extend(recommendations_list)

                # Suggest the closest matching location
                bucket_match = self._find_closest_bucket_match(normalized_path, external_locations)
                if bucket_match:
                    result['recommendations'].append(
                        f"       3. Consider using the location: {bucket_match['name']} ({bucket_match['url']})"
                    )

            return result

        except Exception as e:
            result['recommendations'].append(f"[ERROR] Error during access check: {str(e)}")
            result['accessible'] = None  # Unknown state
            logging.error(f"Error during access check: {str(e)}")
            return result

    def _find_closest_bucket_match(self, s3_path, locations):
        """
        Find external location with the same bucket

        Parameters:
        - s3_path: Target S3 path
        - locations: List of external locations

        Returns:
        - dict: Matching location or None
        """
        try:
            # Extract bucket name from path (case insensitive)
            bucket_pattern = r's3://([^/]+)'
            match = re.match(bucket_pattern, s3_path, re.IGNORECASE)
            if not match:
                return None

            target_bucket = match.group(1).lower()

            # Find location with same bucket (case insensitive)
            for location in locations:
                loc_match = re.match(bucket_pattern, location['url'], re.IGNORECASE)
                if loc_match and loc_match.group(1).lower() == target_bucket:
                    return location

        except Exception:
            pass

        return None
