import os
from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator


class FileModifiedSensor(BaseSensorOperator):

    def __init__(self, file_path, variable_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.variable_key = variable_key

    def poke(self, context):

        if not os.path.exists(self.file_path):
            self.log.info("File not found, waiting...")
            return False

        current_mtime = os.path.getmtime(self.file_path)

        last_mtime = float(
            Variable.get(self.variable_key, default_var=0.0)
        )

        if current_mtime > last_mtime:
            Variable.set(self.variable_key, str(current_mtime))
            self.log.info("File changed → proceed")
            return True

        self.log.info("No change detected → waiting")
        return False