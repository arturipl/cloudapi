import configparser


class ApiConfiguration:
    def _get_cfg(self, section: str, key: str):
        if section not in self._cfg:
            raise KeyError("Section '{}' not in INI file".format(section))
        if key not in self._cfg[section]:
            raise KeyError("value '{}' not in section '{}' of INI file".format(key, section))
        return self._cfg[section][key]

    @property
    def log_dir(self):
        return str(self._get_cfg('Logging', 'dir'))

    @property
    def log_level(self):
        return str(self._get_cfg('Logging', 'level'))

    @property
    def store_devs(self):
        return str(self._get_cfg('Storage', 'devices database'))

    @property
    def store_emotions(self):
        return str(self._get_cfg('Storage', 'emotion database'))

    def __init__(self, inifile):
        config = configparser.ConfigParser()
        config.read(inifile)

        self._cfg = config
