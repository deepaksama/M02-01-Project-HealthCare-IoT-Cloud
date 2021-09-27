import json

CONFIG_FILE = "./anomaly_config.json"

class ConfigReader:
    def __init__(self):
        self.config = self._read_config()

    def _read_config(self):
        f = open(CONFIG_FILE)
        config = json.loads(f.read())
        f.close()
        return config
    
    def get_config_for_datatype(self, datatype):
        return self.config[datatype]

