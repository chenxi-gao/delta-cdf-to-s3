from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Any, Dict


class FileNameGenerator(ABC):
    """Abstract base class for generating S3 file names"""

    @abstractmethod
    def generate_filename(self, base_params: Dict[str, Any], **kwargs) -> str:
        pass

    @abstractmethod
    def get_required_params(self) -> list:
        pass


class OnyxReportNameGenerator(FileNameGenerator):
    def generate_filename(self, base_params: Dict[str, Any], **kwargs) -> str:
        required_params = self.get_required_params()
        for param in required_params:
            if param not in kwargs:
                raise ValueError(f"Missing required parameter: {param}")

        if 'start_date' not in base_params or 'end_date' not in base_params:
            raise ValueError("Missing required base parameters: start_date and end_date")

        start_date = self._parse_date(base_params['start_date'])
        end_date = self._parse_date(base_params['end_date'])
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        environment = kwargs['environment']
        return f"weekly_report_{start_date_str}_to_{end_date_str}_vermont_{environment}"

    def get_required_params(self) -> list:
        return ['environment']

    def _parse_date(self, date_input) -> datetime:
        if isinstance(date_input, datetime):
            return date_input
        elif isinstance(date_input, date):
            return datetime.combine(date_input, datetime.min.time())
        elif isinstance(date_input, str):
            for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                try:
                    return datetime.strptime(date_input, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse date: {date_input}")
        else:
            raise ValueError(f"Invalid date type: {type(date_input)}")


class FileNameGeneratorFactory:
    _generators = {
        'oynx_report': OnyxReportNameGenerator,
    }

    @classmethod
    def create_generator(cls, generator_type: str) -> FileNameGenerator:
        if generator_type.lower() not in [k.lower() for k in cls._generators.keys()]:
            available_types = list(cls._generators.keys())
            raise ValueError(
                f"Unsupported generator type: {generator_type}. Available types: {available_types}"
            )
        generator_key = None
        for key in cls._generators.keys():
            if key.lower() == generator_type.lower():
                generator_key = key
                break
        return cls._generators[generator_key]()

    @classmethod
    def get_available_generators(cls) -> list:
        return list(cls._generators.keys())

    @classmethod
    def register_generator(cls, name: str, generator_class: type):
        if not issubclass(generator_class, FileNameGenerator):
            raise ValueError("Generator class must inherit from FileNameGenerator")
        cls._generators[name] = generator_class
