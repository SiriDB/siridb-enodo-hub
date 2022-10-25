import json
import logging
import os
import pathlib
from lib.serverstate import ServerState
from version import VERSION


class UpgradeUtil:

    @staticmethod
    async def upgrade_thingsdb():
        logging.info("Looking for upgrades for thingsdb collection...")
        try:
            thingsdb_hub_version = await ServerState.thingsdb_client.query(
                ".hub_version")
        except Exception:
            thingsdb_hub_version = None
        path_to_upgrade_file = os.path.join(
            pathlib.Path(__file__).parent.resolve(),
            "..", "..", "upgrade", "thingsdb.json")

        if not os.path.exists(path_to_upgrade_file):
            logging.info("No upgrade file found...")
            logging.debug(
                f"No upgrade file found at {path_to_upgrade_file}")
            return
        upgrade_dict = {}
        with open(path_to_upgrade_file, 'r') as f:
            upgrade_dict = json.loads(f.read())

        index_of_current_version = None
        index_of_thingsdb_hub_version = -1
        for idx, version in enumerate(upgrade_dict.keys()):
            if version == VERSION:
                index_of_current_version = idx
            if version == thingsdb_hub_version:
                index_of_thingsdb_hub_version = idx

        updates_ran = 0
        for idx, upgrade_data in enumerate(upgrade_dict.values()):
            if index_of_thingsdb_hub_version < idx or \
                    index_of_thingsdb_hub_version == -1:
                updates_ran += 1
                await ServerState.thingsdb_client.query(
                    upgrade_data)
                if idx == index_of_current_version:
                    logging.info(
                        f"Ran {updates_ran} updates to ThingsDB collection...")
                    return
        logging.info(
            f"Ran {updates_ran} updates to ThingsDB collection...")
