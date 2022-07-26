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
        thingsdb_hub_version = await ServerState.storage.\
            get_registered_hub_version()
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
        index_of_thingsdb_hub_version = 0
        for idx, version in enumerate(upgrade_dict.keys()):
            if version == VERSION:
                index_of_current_version = idx
            if version == thingsdb_hub_version:
                index_of_thingsdb_hub_version = idx

        if index_of_current_version is None:
            return
        found_first = False
        updates_ran = 0
        if index_of_thingsdb_hub_version == 0:
            found_first = True
        for idx, upgrade_data in enumerate(upgrade_dict.values()):
            if found_first is True:
                updates_ran += 1
                await ServerState.storage.run_code(
                    upgrade_data.get("upgrade"))
                if idx == index_of_current_version:
                    logging.info(
                        f"Ran {updates_ran} updates to ThingsDB collection...")
                    return
            elif idx == index_of_thingsdb_hub_version:
                found_first = True
        logging.info(
            f"Ran {updates_ran} updates to ThingsDB collection...")
