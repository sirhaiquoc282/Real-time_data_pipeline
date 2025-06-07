import yaml

def load_config():
    try:
        with open("main/configs/config.yml", "r") as f:
            configs = yaml.safe_load(f)
        return configs
    except Exception as e:
        raise Exception("Failed to load configs")
