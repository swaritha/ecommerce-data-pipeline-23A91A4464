import os
import yaml
import sqlalchemy
from sqlalchemy import text

CONFIG_PATH = os.path.join("config", "config.yaml")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def get_engine(config):
    db = config["database"]
    url = f"postgresql+psycopg2://{db['user']}:{db['password']}@" \
          f"{db['host']}:{db['port']}/{db['name']}"
    print("USING DB URL:", url)
    return sqlalchemy.create_engine(url)

def main():
    config = load_config()
    engine = get_engine(config)
    with engine.connect() as conn:
        for table in [
            "production.customers",
            "production.products",
            "production.transactions",
            "production.transactionitems",
        ]:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"{table}: {count}")

if __name__ == "__main__":
    main()
